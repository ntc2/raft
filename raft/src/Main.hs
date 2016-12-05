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
-- - [ ] Track latest client request processed -- with request id and
--   response -- in state machine state; see Section 8 of paper. For
--   this we need the property that the client never reuses a request
--   id until a request with a different id has been processed. But
--   it's OK to use a finite counter that wraps around.
--
--   The assumption is that clients only send one request at a time,
--   resending periodically if they don't hear back.
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
--   state machine). in the leader, this thread also responds to
--   clients.
--
-- - election timer in followers and candidates that sends election
--   events to the main loop (e.g. a "start election" event)
--
-- - heartbeat timer in leaders. This just sends 'AppendEntries' RPCs
--   to followers. in fact, this can be the *only* source of
--   'AppendEntries'. No need to update state (except restart the
--   timer) here; the main loop can do follower book keeping in when
--   processing 'AppendEntriesResponse' RPCs.
--
-- Important property: at most *one* thread. Here we'll have all
-- writes in the main loop, except for state machine updates, and
-- updates to 'lastApplied', which happen in the state machine
-- updater.

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
  -- Not really sure what a good value for the delay is, but this
  -- seems quite conservative.
  let delay = minElectionTimeout `div` 5
  timer <- delayedAction delay $ handleHeartbeatTimeout ss
  CM.void $ CCM.swapMVar (ss_timer ss) timer

-- | Send all followers any log events they don't have yet.
--
-- The empty heartbeats in the paper are the special case where the
-- follower is completely up to date.
handleHeartbeatTimeout :: ServerState cmd -> IO ()
handleHeartbeatTimeout ss = do
  ps <- CCM.readMVar (ss_persistentState ss)
  vs <- CCM.readMVar (ss_volatileState ss)
  Just vls <- CCM.readMVar (ss_volatileLeaderState ss)
  let sids = c_serverIds . ps_config $ ps
  CM.forM_ sids $ \sid ->
    -- HERE
    sendRpc ss sid $ AppendEntries
      { r_term = undefined
      , r_leaderId = undefined
      , r_prevLogIndex = undefined
      , r_prevLogTerm = undefined
      , r_entries = undefined
      , r_leaderCommit = undefined
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
  delay <- SR.randomRIO (minElectionTimeout, 2 * minElectionTimeout)
  timer <- delayedAction delay $ handleElectionTimeout ss
  CM.void $ CCM.swapMVar (ss_timer ss) timer

restartElectionTimer :: ServerState cmd -> IO ()
restartElectionTimer ss = do
  cancelTimer ss
  startElectionTimer ss

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
  restartElectionTimer ss
  requestVotes
  where
    incrementTerm =
      modifyPersistentState ss
        -- TODO: try simplifying with lenses?
        (\ps -> ps { ps_currentTerm = 1 + ps_currentTerm ps })
    voteForSelf = do
      modifyPersistentState ss $ \ps ->
        ps { ps_votedFor = ps_myServerId ps }
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
      let sids = c_serverIds . ps_config $ ps
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
handleRpc :: ServerState cmd -> Rpc cmd -> IO ()
handleRpc ss rpc = do
  case rpc of
    AppendEntries {} -> undefined
    AppendEntriesResponse {} -> undefined
    RequestVote {} -> undefined
    RequestVoteResponse {} -> handleRequestVoteResponse
  where
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
        let votes = Set.insert (ps_myServerId ps) (vcs_votesReceived vcs)
        CM.void $ CCM.swapMVar (ss_volatileCandidateState ss)
          (Just $ vcs { vcs_votesReceived = votes })
        let numServers = length . c_serverIds . ps_config $ ps
        let numVotes = Set.size votes
        CM.when (2 * numVotes > numServers) $
          becomeLeader ss

-- | Become a leader.
--
-- See page 4 ...
becomeLeader :: ServerState cmd -> IO ()
becomeLeader ss = do
  undefined

----------------------------------------------------------------
-- * Types
----------------------------------------------------------------

type Term = Integer
-- | Treat this opaquely, so I can change it later, e.g. to add
-- networking. Or, could always use integers to represent server ids,
-- and have a separate abstraction that contacts servers using their
-- ids. This might be better! E.g.,
--
-- > send :: ServerId -> RPC -> m ()
type ServerId = Integer
type Index = Integer

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

data PersistentState cmd
  = PersistentState
    { ps_currentTerm :: !Term
    , ps_votedFor :: !ServerId
      -- Use something more efficient later.
    , ps_log :: ![LogEntry cmd]
      -- Id of this server.
    , ps_myServerId :: !ServerId
      -- Persistent in case we implement live configuration changes
      -- later.
    , ps_config :: !Config
    }
  deriving ( Show )

data VolatileState
  = VolatileState
    { vs_commitIndex :: !Index
    , vs_lastApplied :: !Index
    }
  deriving ( Show )

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
    { c_serverIds :: ![ServerId] -- ^ The set of all server ids.
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
    , ss_volatileState :: !(MVar VolatileState)
    , ss_volatileLeaderState :: !(MVar (Maybe VolatileLeaderState))
    , ss_volatileCandidateState :: !(MVar (Maybe VolatileCandidateState))
    }
