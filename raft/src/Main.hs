{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}
module Main where

import qualified Control.Concurrent as CC
import           Control.Concurrent.Async ( Async )
import qualified Control.Concurrent.Async as CCA
import           Control.Concurrent.Chan ( Chan )
import qualified Control.Concurrent.Chan as CCC
import           Control.Concurrent.MVar ( MVar )
import qualified Control.Concurrent.MVar as CCM
import qualified Control.Monad as CM
import           Data.Map.Strict ( Map )
import qualified Data.Map.Strict as Map
import           Data.Set ( Set )
import qualified Data.Set as Set
import qualified System.Random as SR

main :: IO ()
main = do
  putStrLn "hello world"


----------------------------------------------------------------
-- * Client
----------------------------------------------------------------
--
-- Clients send one request at a time, and keep sending it until they
-- receive a response. We can probably relax this and allow clients to
-- give up on a request, but they can't resend an earlier request
-- again after a later request. Each client request includes a request
-- ID, which should be locally unique for that client (e.g. it can
-- wrap over long time periods, but requests close together in time
-- should have different ids).
--
-- Clients contact the leader if it's known, and a random server
-- otherwise. After sending a request and not receiving a response,
-- they contact a random server, in case the leader died. Followers
-- redirect clients to the leader, and otherwise ignore client
-- requests.

----------------------------------------------------------------
-- * Server
----------------------------------------------------------------
--
-- What does a server need?
--
-- - Server id.
--
-- - Main event loop that handles incoming server and client rpcs.
--
-- - A notion of current role and corresponding event loops /
--   timeouts. The server can change what loops it runs on role
--   transitions, and can decide how to handle incoming client and
--   server rpcs based on this role.
--
--   - Leader:
--
--     - Sends out heartbeats periodically.
--
--       - [X] How is the heartbeat delta determined?
--
--         A: I can't find any discussion of this in the paper, but
--         some fraction of 'minElectionTimeout' should suffice.
--
--   - Follower:
--
--     - Becomes candidate if heartbeat is not received during timeout.
--
--       - [X] How is the election timeout determined? Iirc it's
--         randomly chosen, but I don't remember if that's every time,
--         or once per server (presumably every time ...).
--
--         A: going with "every time".
--
--   - Candidate:
--
--     - Start new election if no leader elected in time, or at all
--       (can just check for at all?).  The election timeout is the
--       same as the heartbeat timeout for followers.
--
--   To facilitate testing, I want some additional, orthogonal
--   roles. Better to have these in a separate role type from
--   'ServerRole'. E.g., I can have an online role and an offline
--   role, and a special RPC for making servers crash and come back
--   up. For network errors, I could handle these in the server, or in
--   the send/receive functions. Unclear what's better. What's the
--   difference:
--
--   - global config: the send/receive functions look up in their
--     global config whether they should send/receive a given message.
--
--   - local config: the code for deciding whether to send or receive
--     can be mixed into the other logic. This might allow for more
--     complicated tests, but will also clutter the code.
--
--   Conclusion: make the network error config only known to the
--   send/receive functions. Unclear if it should be per server or
--   not, however.
--
-- ----------------------------------------------------------------
-- Tasks
--
-- - [X] Implement resetable timer loop. E.g., for the leader, want a
--   loop that determines when to send heartbeats, that is reset for
--   any other communication with corresponding follower. But to
--   start, can just send heartbeats regardless of whether they are
--   necessary and save the resets for later optimization. But for
--   followers and candidates we really do need an interruptible
--   election timer.
--
--   Properties:
--
--   - timer counts down and fires an event if it reaches zero. It
--     also restarts if it reaches zero.
--
--   - timer can be canceled / reset without firing its event.
--
-- - [ ] Implement server initialization code.
--
--   - [ ] Initial state.
--
--   - [ ] Initial config.
--
--   - [ ] Setup whatever 'sendRpc' needs.
--
-- - [ ] Implement state machine updater event loop.
--
-- - [ ] Track latest client request processed -- with request id and
--   response -- in state machine state; see Section 8 of paper. For
--   this we need the property that the client never reuses a request
--   id until a request with a different id has been processed. But
--   it's OK to use a finite counter that wraps around.
--
--   The assumption is that clients only send one request at a time,
--   resending periodically if they don't hear back.
--
-- - [ ] use 'Data.Sequence' instead of a linked list for the event
--   log 'ps_log'. Note that 'Data.Vector' is not a good choice here,
--   since we want efficient lookups and appends, and appends are
--   @O(n)@ for 'Data.Vector'.
--
-- ----------------------------------------------------------------
-- Thoughts on concurrency
--
-- I started by coding as if every event were handled concurrently. It
-- would probably be easier to decide on what concurrency is allowed
-- -- and this should be the minimum amount that gives sufficient
-- performance/responsiveness -- and only protect myself from that
-- amount of concurrency. One pitfall here is that if I want to add
-- more concurrency later, I may have to change code in more places
-- when the assumptions change.
--
-- A realistic amount of concurrency:
--
-- Use synchronized queues to communicate between different event
-- driven threads. Threads:
--
-- - main loop handling rpcs and election events (below). The main
--   loop sends events to the state machine updater thread, whenever
--   it observes that 'commitIndex > lastApplied'
--
-- - state machine updater (because it could be slow for a complicated
--   state machine). In the leader, this thread also responds to
--   clients.
--
-- - election timer in followers and candidates that sends election
--   events to the main loop (e.g. a "start election" event)
--
-- - heartbeat timer in leaders. This just sends 'AppendEntries' RPCs
--   to followers. In fact, this can be the *only* source of
--   'AppendEntries'. No need to update state (except restart the
--   timer) here; the main loop can do follower book keeping when
--   processing 'AppendEntriesResponse' RPCs.
--
-- Important property: at most *one* thread *writes* each piece of
-- state. Here we'll have all writes in the main loop, except for
-- state machine updates, and updates to 'lastApplied', which happen
-- in the state machine updater.
--
-- We could enforce this "single writer" property more formally by not
-- sharing any mutable state between the different threads: rather,
-- the threads that write state could compute the new values, and then
-- broadcast these new values back to the other reader threads. Each
-- thread could then use a state monad, and update values written by
-- other threads when the new values arrive in the queue for the given
-- thread.
--
-- Trade offs when using mutable vars (e.g. 'IORef's, 'MVar's,
-- 'TVar's) for single-writer mutable state:
--
-- - (+) easier to communicate state updates to other threads, since
--   it happens automatically.
--
-- - (+) state updates in other threads happen sooner: if we queued
--   state updates, then there could be many intermediate events
--   between the event that triggered a state update and the event
--   that committed it in another thread.
--
-- - (-) introduces concurrent updates in other threads, which could
--   cause bugs if we're not careful.
--
-- Best of both worlds (?): use mutable vars, but also use a state or
-- reader monad, and read current value of all mutable var state at
-- the beginning of handling each queue event in each thread. Then
-- mutable var updates happen automatically, but only at predictable
-- times (i.e. at the beginning of handling each queue event). A
-- potential downside here is that keeping track of updates to mutable
-- vars would be more complicated: in writer threads, we now have two
-- copies of the mutable state, one in the state/reader monad and one
-- in the 'MVar's.

----------------------------------------------------------------
-- * Delayed actions
----------------------------------------------------------------

-- | Run a delayed action, returning a handle that can be used to
-- cancel the delayed action -- using 'CCA.cancel' -- if it has not
-- run yet.
--
-- - @delay@: time to delay in milliseconds.
-- - @action@: the action to run after the delay.
delayedAction :: Int -> IO () -> IO (Async ())
delayedAction delay action = do
  CCA.async $ CC.threadDelay delay >> action

cancelTimer :: ServerState cmd -> IO ()
cancelTimer ss = CCA.cancel =<< CCM.readMVar (ss_timer ss)

----------------------------------------------------------------
-- * Heartbeat timer
----------------------------------------------------------------

startHeartbeatTimer :: ServerState cmd -> IO ()
startHeartbeatTimer ss = do
  cancelTimer ss
  -- Not really sure what a good value for the delay is, but this
  -- seems quite conservative.
  let delay = minElectionTimeout `div` 5
  timer <- delayedAction delay $ sendAppendEntriesToAllFollowers ss
  CM.void $ CCM.swapMVar (ss_timer ss) timer

-- | Send all followers any log events they don't have yet.
--
-- The empty heartbeats in the paper are the special case where the
-- follower is completely up to date.
sendAppendEntriesToAllFollowers :: ServerState cmd -> IO ()
sendAppendEntriesToAllFollowers ss = do
  ps <- CCM.readMVar (ss_persistentState ss)
  vs <- CCM.readMVar (ss_volatileState ss)
  Just vls <- CCM.readMVar (ss_volatileLeaderState ss)
  let sids = c_otherServerIds . ps_config $ ps
  CM.forM_ sids $ \sid -> do
    -- Figure out what entries the follower doesn't have yet.
    let Just nextIndex =
          fromIntegral . unIndex <$> Map.lookup sid (vls_nextIndex vls)
    let entries = drop nextIndex $ ps_log ps
    let LogEntry prevLogIndex prevLogTerm _ = ps_log ps !! nextIndex
    sendRpc ss sid $ AppendEntries
      { r_term = ps_currentTerm ps
      , r_leaderId = ps_myServerId ps
      , r_prevLogIndex = prevLogIndex
      , r_prevLogTerm = prevLogTerm
      , r_entries = entries
      , r_leaderCommit = vs_commitIndex vs
      }

----------------------------------------------------------------
-- * Election timer
----------------------------------------------------------------

-- | The election timeout is randomly chosen between 'minElectionTimeout'
-- and @2 * minElectionTimeout@, on each iteration of the election loop.
minElectionTimeout :: Int
minElectionTimeout = 150 -- Paper suggestion.

startElectionTimer :: ServerState cmd -> IO ()
startElectionTimer ss = do
  cancelTimer ss
  delay <- SR.randomRIO (minElectionTimeout, 2 * minElectionTimeout)
  timer <- delayedAction delay $ handleElectionTimeout ss
  CM.void $ CCM.swapMVar (ss_timer ss) timer

handleElectionTimeout :: ServerState cmd -> IO ()
handleElectionTimeout ss = do
  role <- CCM.readMVar $ ss_role ss
  case role of
    Leader -> error "The leader should not be running an election timer!"
    Follower -> CCC.writeChan (ss_mainQueue ss) StartElection
    Candidate -> CCC.writeChan (ss_mainQueue ss) StartElection

-- | Start an election with our self as candidate.
--
-- Things are getting a little tricky here: I'm assuming that the role
-- mvar was consumed and is now empty; this function will fill it
-- again.
--
-- Election process (Figure 2):
--
-- - increment current term
-- - vote for self
-- - reset election timer
-- - send request vote rpc to other servers
--
-- The main loop (elsewhere) then waits for three possible outcomes,
-- which are handled separately:
--
-- 1. If we get majority of votes, convert to leader.
--
-- 2. If we receive append entries rpc from new leader, become
--    follower.
--
-- 3. If election timeout, try again.
startElection :: ServerState cmd -> IO ()
startElection ss = do
  CM.void $ CCM.swapMVar (ss_role ss) Candidate
  incrementTerm
  voteForSelf
  startElectionTimer ss
  requestVotes
  where
    incrementTerm =
      modifyPersistentState ss
        -- TODO: try simplifying with lenses?
        (\ps -> ps { ps_currentTerm = 1 + ps_currentTerm ps })
    voteForSelf = do
      modifyPersistentState ss $ \ps ->
        ps { ps_votedFor = Just $ ps_myServerId ps }
      ps <- CCM.readMVar (ss_persistentState ss)
      -- Initialize the volatile candidate state and count our self
      -- vote. If we add more fields to the volatile candidate state,
      -- then create a separate initialization function.
      CM.void $ CCM.swapMVar (ss_volatileCandidateState ss)
        (Just $ VolatileCandidateState
          { vcs_votesReceived = Set.singleton $ ps_myServerId ps })
    requestVotes = do
      ps <- CCM.readMVar (ss_persistentState ss)
      let LogEntry lastLogIndex lastLogTerm _cmd = last $ ps_log ps
      let sids = c_otherServerIds . ps_config $ ps
      CM.forM_ sids $ \sid ->
        sendRpc ss sid $ RequestVote
          { r_term = ps_currentTerm ps
          , r_candidateId = ps_myServerId ps
          , r_lastLogIndex = lastLogIndex
          , r_lastLogTerm = lastLogTerm
          }

----------------------------------------------------------------
-- * Persistent state
----------------------------------------------------------------

-- | The persistent state is modified through this function, not
-- directly by callers of this function, so that we can handle
-- persistence in this one place.
modifyPersistentState :: ServerState cmd -> (PersistentState cmd -> PersistentState cmd) -> IO ()
modifyPersistentState ss f = do
  CCM.modifyMVar_ (ss_persistentState ss) (return . f)
  persist ss
  where
    -- Not actually persisting anything right now. For testing, I'm
    -- just going to pause server threads, not actually kill them, so
    -- I don't really need persistent storage here.
    persist _ss = return ()

----------------------------------------------------------------
-- * RPCs
----------------------------------------------------------------

-- | Send an RPC to another server.
sendRpc :: ServerState cmd -> ServerId -> Rpc cmd -> IO ()
sendRpc ss sid rpc = do
  -- Plan: have a mapping from 'ServerId' to 'ServerState' and put the
  -- rpc on the corresponding main loop queue.
  undefined ss sid rpc

----------------------------------------------------------------
-- * Main event loop
----------------------------------------------------------------

mainLoop :: ServerState cmd -> IO ()
mainLoop ss = do
  me <- CCC.readChan (ss_mainQueue ss)
  handleMainEvent ss me
  mainLoop ss

handleMainEvent :: ServerState cmd -> MainEvent cmd -> IO ()
handleMainEvent ss (Rpc rpc) = handleRpc ss rpc
handleMainEvent ss StartElection = startElection ss

-- | Receive an RPC from another server.
--
-- This is the part of the main loop that processes RPCs.
--
-- With all RPCs, if the term in the RPC is greater than our term,
-- then we should update our term and become a follower.
handleRpc :: ServerState cmd -> Rpc cmd -> IO ()
handleRpc ss rpc = do
  case rpc of
    AppendEntries {} -> undefined
    AppendEntriesResponse {} -> undefined
    RequestVote {} -> handleRequestVote
    RequestVoteResponse {} -> handleRequestVoteResponse
  where
    -- Cases to consider:
    --
    -- - rpc term is older than our current term: reject.
    --
    -- - rpc term is equal to our current term: check to see if we've
    --   already voted, and vote accordingly.
    --
    -- - rpc term is greater than our current term: revert to
    --   follower, clear 'ps_votedFor', and vote accordingly.
    --
    -- Above "vote accordingly" means to be consistent with any
    -- existing vote and check the "at least as up to date as"
    -- property for new votes.
    handleRequestVote = do
      ps <- CCM.readMVar (ss_persistentState ss)
      case r_term rpc `compare` ps_currentTerm ps of
        LT -> sendRpc ss (r_candidateId rpc)
              RequestVoteResponse
              { r_term = ps_currentTerm ps
              , r_voteGranted = False
              , r_responderId = ps_myServerId ps
              }
        EQ -> voteAccordingly
        GT -> do
          becomeFollower ss (r_term rpc)
          voteAccordingly
    -- Assumes that current term and rpc term are the same. The
    -- @becomeFollower@ takes care of this.
    voteAccordingly = do
      ps <- CCM.readMVar (ss_persistentState ss)
      case ps_votedFor ps of
        -- Already voted.
        Just voteId -> sendRpc ss (r_candidateId rpc)
                    RequestVoteResponse
                    { r_term = r_term rpc
                    , r_voteGranted = voteId == r_candidateId rpc
                    , r_responderId = ps_myServerId ps
                    }
        -- Haven't voted.
        Nothing -> do
          -- See Figure 2 and end of Section 5.4.1.
          let candidateIsUpToDate =
                let LogEntry lastLogIndex lastLogTerm _ = last $ ps_log ps
                in r_lastLogTerm rpc > lastLogTerm ||
                   ( r_lastLogTerm rpc == lastLogTerm &&
                     r_lastLogIndex rpc >= lastLogIndex )
          CM.when candidateIsUpToDate $ do
            modifyPersistentState ss $ \ps ->
              ps { ps_votedFor = Just $ r_candidateId rpc }
          sendRpc ss (r_candidateId rpc)
            RequestVoteResponse
            { r_term = r_term rpc
            , r_voteGranted = candidateIsUpToDate
            , r_responderId = ps_myServerId ps
            }

    -- Cases to consider:
    --
    -- - vote was granted and vote term is equal to our term: count
    --   vote and convert to leader if we have a majority.
    --
    -- - vote was granted and vote term is not equal to our term:
    --   ignore stale vote.
    --
    -- - vote was not granted: check the vote term. If it's greater
    --   than our term, update our term and stop being a candidate? Or
    --   just ignore the vote: we should receive a request vote or
    --   append entries rpc soon enough ...
    --
    -- If we're not a candidate, then we ignore the vote.
    handleRequestVoteResponse = do
      role <- CCM.readMVar (ss_role ss)
      ps <- CCM.readMVar (ss_persistentState ss)
      CM.when (role == Candidate &&
            r_voteGranted rpc &&
            r_term rpc == ps_currentTerm ps) $ do
        Just vcs <- CCM.readMVar (ss_volatileCandidateState ss)
        let votes = Set.insert (r_responderId rpc) (vcs_votesReceived vcs)
        CM.void $ CCM.swapMVar (ss_volatileCandidateState ss)
          (Just $ vcs { vcs_votesReceived = votes })
        let numServers = (1 +) . length . c_otherServerIds . ps_config $ ps
        let numVotes = Set.size votes
        -- Become leader if a majority of servers voted for us.
        CM.when (2 * numVotes > numServers) $
          becomeLeader ss

-- | Become follower in given term.
--
-- If the new term is the same as the old term, then we preserve any
-- existing vote. Otherwise, we reset our vote to 'Nothing'.
becomeFollower :: ServerState cmd -> Term -> IO ()
becomeFollower ss newTerm = do
  startElectionTimer ss
  CM.void $ CCM.swapMVar (ss_role ss) Follower
  currentTerm <- ps_currentTerm <$> CCM.readMVar (ss_persistentState ss)
  case currentTerm `compare` newTerm of
    LT -> modifyPersistentState ss $ \ps ->
      ps { ps_votedFor = Nothing
         , ps_currentTerm = newTerm
         }
    -- We preserve our vote, and the term is already correct.
    EQ -> return ()
    GT -> error "becomeFollower: newTerm is in the past!"

-- | Become a leader.
--
-- See page 4.
becomeLeader :: ServerState cmd -> IO ()
becomeLeader ss = do
  startHeartbeatTimer ss
  initializeVolatileLeaderState
  CM.void $ CCM.swapMVar (ss_role ss) Leader
  sendAppendEntriesToAllFollowers ss
  where
    initializeVolatileLeaderState = do
      ps <- CCM.readMVar (ss_persistentState ss)
      let sids = c_otherServerIds . ps_config $ ps
      let constMap v = Map.fromList [ (s, v) | s <- sids ]
      let LogEntry lastLogEntryIndex _ _ = last $ ps_log ps
      CM.void $ CCM.swapMVar (ss_volatileLeaderState ss) $
        Just $ VolatileLeaderState
        { vls_nextIndex = constMap (lastLogEntryIndex + 1)
        , vls_matchIndex = constMap 0
        }

----------------------------------------------------------------
-- * Types
----------------------------------------------------------------

-- | Use a @newtype@ to decrease the chance of accidentally
-- interchanging a 'Term' and an 'Index', since they are both
-- 'Integer' and used in similar places.
newtype Term = Term { unTerm :: Integer }
  deriving ( Eq, Num, Ord, Show )
-- | Treat this opaquely, so I can change it later, e.g. to add
-- networking. Or, could always use integers to represent server ids,
-- and have a separate abstraction that contacts servers using their
-- ids. This might be better! E.g.,
--
-- > send :: ServerId -> RPC -> m ()
type ServerId = Integer
-- | See 'Term' above.
newtype Index = Index { unIndex :: Integer }
  deriving ( Eq, Num, Ord, Show )

-- | Log entries are parameterized by the command type @cmd@. E.g.,
-- for a key-value store service as used in the paper, the command
-- type could be
--
-- > data KvCmd = Get Key | Put Key Value
data LogEntry cmd = LogEntry Index Term cmd
  deriving ( Show )

{-
data KvCmd = Get Key | Put Key Value
type KvLogEntry = LogEntry KvCmd
-}

-- | State-machine state.
--
-- Depends on the underlying commands.
type family SmState cmd :: *

data PersistentState cmd
  = PersistentState
    { ps_currentTerm :: !Term
    , ps_votedFor :: !(Maybe ServerId)
      -- | Raft uses 1-based log indexing, and @(0,0)@ is the
      -- @(index,term)@ for the entry before the first. So, we assume
      -- that 'ps_log' has been initialized with an initial dummy
      -- 'LogEntry' with index and term equal to 0, and a dummy
      -- command. This eliminates special cases involving the log
      -- entry at index 1, i.e. the first real log entry.
    , ps_log :: ![LogEntry cmd]
      -- | Id of this server.
    , ps_myServerId :: !ServerId
      -- | Persistent in case we implement live configuration changes
      -- later.
    , ps_config :: !Config
    }
  deriving ( Show )

data VolatileState cmd
  = VolatileState
    { vs_commitIndex :: !Index
    , vs_lastApplied :: !Index
    , vs_smState :: !(SmState cmd)
    }
deriving instance Show (SmState cmd) => Show (VolatileState cmd)

data VolatileLeaderState
  = VolatileLeaderState
    { vls_nextIndex :: !(Map ServerId Index)
    , vls_matchIndex :: !(Map ServerId Index)
    }
  deriving ( Show )

data VolatileCandidateState
  = VolatileCandidateState
    { vcs_votesReceived :: !(Set ServerId)
    }
  deriving ( Show )

data Rpc cmd
  = AppendEntries
    { r_term :: !Term
    , r_leaderId :: !ServerId
    , r_prevLogIndex :: !Index
    , r_prevLogTerm :: !Term
    , r_entries :: ![LogEntry cmd]
    , r_leaderCommit :: !Index
    }
  | AppendEntriesResponse
    { r_term :: !Term
    , r_success :: !Bool
    , r_responderId :: !ServerId -- ^ Not in paper.
    }
  | RequestVote
    { r_term :: !Term
    , r_candidateId :: !ServerId
    , r_lastLogIndex :: !Index
    , r_lastLogTerm :: !Term
    }
  | RequestVoteResponse
    { r_term :: !Term
    , r_voteGranted :: !Bool
    , r_responderId :: !ServerId -- ^ Not in paper.
    }
  deriving ( Show )

data ServerRole
  = Candidate
  | Follower
  | Leader
  deriving ( Eq, Show )

-- | The configuration of the collection of servers.
--
-- The Raft paper describes a protocol for updating the
-- configuration. For now we're going to assume it's constant for the
-- life of the servers.
data Config
  = Config
    { c_otherServerIds :: ![ServerId]
      -- ^ The set of server IDs for all servers *not* including us.
    }
  deriving ( Show )

-- | Event for the main queue.
data MainEvent cmd
  = Rpc (Rpc cmd)
  | StartElection
  deriving ( Show )

-- | Event for the state machine queue.
data SMEvent
  = UpdateSM
  deriving ( Show )

-- | All of the server state.
--
-- I'm using 'MVar's everywhere, but most of them have a single
-- writer; I could change those to 'IORef's, but I don't see the
-- benefit.
data ServerState cmd
  = ServerState
    { ss_timer :: !(MVar (Async ()))
      -- ^ Cancelable action that runs after a delay. This variable
      -- has multiple writers, but we could just as well use an
      -- 'IORef' here as writes correspond to restarting the timer.
    , ss_mainQueue :: !(Chan (MainEvent cmd))
      -- ^ Main loop event queue.
    , ss_smQueue :: !(Chan SMEvent)
      -- ^ State machine event queue.
    , ss_role :: !(MVar ServerRole)
    , ss_persistentState :: !(MVar (PersistentState cmd))
    , ss_volatileState :: !(MVar (VolatileState cmd))
    , ss_volatileLeaderState :: !(MVar (Maybe VolatileLeaderState))
    , ss_volatileCandidateState :: !(MVar (Maybe VolatileCandidateState))
    }
