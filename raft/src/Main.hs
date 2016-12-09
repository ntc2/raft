{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE ScopedTypeVariables #-}
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
import qualified Control.Exception.Safe as CES
import qualified Control.Monad as CM
import qualified Data.List as DL
import           Data.Map.Strict ( Map )
import qualified Data.Map.Strict as Map
import           Data.Proxy
import           Data.Set ( Set )
import qualified Data.Set as Set
import qualified System.Random as SR
import           Text.Printf ( PrintfType, printf )

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
--   - [ ] Setup whatever 'ss_sendRpc' needs.
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
  CCA.async $ CC.threadDelay (1000 * delay) >> action

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
  let sids = c_otherServerIds . ps_config $ ps
  CM.forM_ sids $ \sid -> do
    sendAppendEntriesToOneFollower ss sid

sendAppendEntriesToOneFollower :: ServerState cmd -> ServerId -> IO ()
sendAppendEntriesToOneFollower ss sid = do
  ps <- CCM.readMVar (ss_persistentState ss)
  vs <- CCM.readMVar (ss_volatileState ss)
  Just vls <- CCM.readMVar (ss_volatileLeaderState ss)
  -- Figure out what entries the follower doesn't have yet.
  let Just nextIndex =
        fromIntegral . unIndex <$> Map.lookup sid (vls_nextIndex vls)
  let entries = drop nextIndex $ ps_log ps
  -- The "previous" entry is the one before the "next" entry.
  let LogEntry prevLogIndex prevLogTerm _ = ps_log ps !! (nextIndex - 1)
  ss_sendRpc ss sid $ AppendEntries
    { r_term = ps_currentTerm ps
    , r_leaderId = c_myServerId . ps_config $ ps
    , r_prevLogIndex = prevLogIndex
    , r_prevLogTerm = prevLogTerm
    , r_entries = entries
    , r_leaderCommit = vs_commitIndex vs
    }

----------------------------------------------------------------
-- * Election timer
----------------------------------------------------------------

-- | The election timeout is randomly chosen between
-- 'minElectionTimeout' and @2 * minElectionTimeout@, on each
-- iteration of the election loop.
--
-- Paper suggests 150. Larger values are useful for testing.
minElectionTimeout :: Int
minElectionTimeout = 3000

startElectionTimer :: ServerState cmd -> IO ()
startElectionTimer ss = do
  cancelTimer ss
  delay <- SR.randomRIO (minElectionTimeout, 2 * minElectionTimeout)
  debug ss $ printf "startElectionTimer: delay %i" delay
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
        ps { ps_votedFor = Just $ c_myServerId . ps_config $ ps }
      ps <- CCM.readMVar (ss_persistentState ss)
      -- Initialize the volatile candidate state and count our self
      -- vote. If we add more fields to the volatile candidate state,
      -- then create a separate initialization function.
      CM.void $ CCM.swapMVar (ss_volatileCandidateState ss)
        (Just $ VolatileCandidateState
          { vcs_votesReceived = Set.singleton $ c_myServerId . ps_config $ ps })
    requestVotes = do
      ps <- CCM.readMVar (ss_persistentState ss)
      let LogEntry lastLogIndex lastLogTerm _cmd = last $ ps_log ps
      let sids = c_otherServerIds . ps_config $ ps
      CM.forM_ sids $ \sid ->
        ss_sendRpc ss sid $ RequestVote
          { r_term = ps_currentTerm ps
          , r_candidateId = c_myServerId . ps_config $ ps
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
-- * State machine loop
----------------------------------------------------------------
--
-- Responsible for updating the state machine state and responding to
-- clients.

smLoop :: ServerState cmd -> IO ()
smLoop ss = CM.forever $ do
  sme <- CCC.readChan (ss_smQueue ss)
  handleSmEvent ss sme

handleSmEvent :: ServerState cmd -> SmEvent -> IO ()
handleSmEvent ss sme = do
  -- TODO
  debug ss $
    printf "dropping SmEvent '%s'." (show sme)

----------------------------------------------------------------
-- * Main event loop
----------------------------------------------------------------

mainLoop :: ServerState cmd -> IO ()
mainLoop ss = CM.forever $ do
  me <- CCC.readChan (ss_mainQueue ss)
  handleMainEvent ss me

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
    AppendEntries {} -> handleAppendEntries
    AppendEntriesResponse {} -> handleAppendEntriesResponse
    RequestVote {} -> handleRequestVote
    RequestVoteResponse {} -> handleRequestVoteResponse
  where
    -- Cases to consider:
    --
    -- - rpc term is older than our current term: reject.
    --
    -- - rpc term is equal to our current term:
    --
    --   - if we're a candidate, then assume a new leader has been
    --     elected and become a follower. Then handle as in follower
    --     case below.
    --
    --   - if we're a leader, then the sky is falling.
    --
    --   - if we're a follower, then implement "receiver
    --     implementation" in Figure 2.
    --
    -- - rpc term is newer than our current term:
    --
    --   - become a follower and implement "receiver implementation"
    --     in Figure 2.
    handleAppendEntries = do
      ps <- CCM.readMVar (ss_persistentState ss)
      role <- CCM.readMVar (ss_role ss)
      case r_term rpc `compare` ps_currentTerm ps of
        LT -> ss_sendRpc ss (r_leaderId rpc)
              AppendEntriesResponse
              { r_term = ps_currentTerm ps
              , r_success = False
              , r_responderId = c_myServerId . ps_config $ ps
                -- This field is not meaningful in this case.
              , r_nextLogIndex = r_lastLogIndex rpc
              }
        EQ | role == Leader -> do
               error "handleAppendEntries: there are two leaders!"
           | role == Candidate -> do
               becomeFollower ss (r_term rpc)
               handleAppendEntriesForCurrentTermAsFollower
           | role == Follower -> do
               handleAppendEntriesForCurrentTermAsFollower
        GT -> do
          becomeFollower ss (r_term rpc)
          handleAppendEntriesForCurrentTermAsFollower
    -- Handle append-entries rpc for case where we believe the rpc is
    -- from the current leader and our term is (now) equal to the
    -- rpc's term.
    handleAppendEntriesForCurrentTermAsFollower = do
      ps <- CCM.readMVar (ss_persistentState ss)
      let lastLogIndex = le_index . last . ps_log $ ps
      let r_prevLogIndex' =
            fromIntegral . unIndex . r_prevLogIndex $ rpc
      -- Making essential use of laziness here: this entry is out of
      -- bounds when `maxLogIndex` is less than `r_prevLogIndex rpc`.
      let LogEntry prevLogIndex prevLogTerm _ =
            ps_log ps !! r_prevLogIndex'
      if lastLogIndex < r_prevLogIndex rpc ||
         prevLogIndex /= r_prevLogIndex rpc ||
         prevLogTerm /= r_prevLogTerm rpc
        then do
        -- Can optimize here by sending back more information to
        -- leader that allows them to do better than simply
        -- decrementing `r_prevLogIndex` by one; see the end of
        -- Section 5.3. However, the paper says this optimization is
        -- not useful in practice.
        ss_sendRpc ss (r_leaderId rpc)
          AppendEntriesResponse
          { r_term = r_term rpc
          , r_success = False
          , r_responderId = c_myServerId . ps_config $ ps
          -- This is the non-optimized part.
          , r_nextLogIndex = r_lastLogIndex rpc
          }
        else do
        let newLog = take (r_prevLogIndex' + 1) (ps_log ps) ++
                     r_entries rpc
        modifyPersistentState ss $ \ps -> ps { ps_log = newLog }
        vs <- CCM.readMVar (ss_volatileState ss)
        let newLastLogIndex = le_index $ last newLog
        CM.when (r_leaderCommit rpc > vs_commitIndex vs) $ do
          CM.void $ CCM.swapMVar (ss_volatileState ss) $
            vs { vs_commitIndex = r_leaderCommit rpc `min`
                                  newLastLogIndex
               }
          CCC.writeChan (ss_smQueue ss) UpdateSm
        ss_sendRpc ss (r_leaderId rpc)
          AppendEntriesResponse
          { r_term = r_term rpc
          , r_success = True
          , r_responderId = c_myServerId . ps_config $ ps
          , r_nextLogIndex = newLastLogIndex + 1
          }

    -- Cases to consider:
    --
    -- - we're not the leader: ignore.
    --
    -- - success field is false: check term field, revert to follower
    --   if greater than current term, and decrement last index for
    --   responder if equal to current term.
    --
    -- - success field is true: check term field and update volatile
    --   leader state match index and last index if equal to current
    --   term.
    handleAppendEntriesResponse = do
      role <- CCM.readMVar (ss_role ss)
      ps <- CCM.readMVar (ss_persistentState ss)
      case role of
        Leader -> do
          Just vls <- CCM.readMVar (ss_volatileLeaderState ss)
          if r_success rpc
            then do
            CM.when (r_term rpc == ps_currentTerm ps) $ do
              let nextIndex' = Map.insert (r_responderId rpc)
                               (r_nextLogIndex rpc)
                               (vls_nextIndex vls)
              let matchIndex' = Map.insert (r_responderId rpc)
                                (r_nextLogIndex rpc - 1)
                                (vls_matchIndex vls)
              CM.void $ CCM.swapMVar (ss_volatileLeaderState ss) $
                Just $ vls { vls_nextIndex = nextIndex'
                           , vls_matchIndex = matchIndex'
                           }
            else
            case r_term rpc `compare` ps_currentTerm ps of
              LT -> return ()
              EQ -> do
                let nextIndex' = Map.update (Just . subtract 1)
                                 (r_responderId rpc)
                                 (vls_nextIndex vls)
                CM.void $ CCM.swapMVar (ss_volatileLeaderState ss) $
                  Just $ vls { vls_nextIndex = nextIndex' }
                sendAppendEntriesToOneFollower ss (r_responderId rpc)
              GT -> becomeFollower ss (r_term rpc)
        _ -> return ()

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
        LT -> ss_sendRpc ss (r_candidateId rpc)
              RequestVoteResponse
              { r_term = ps_currentTerm ps
              , r_voteGranted = False
              , r_responderId = c_myServerId . ps_config $ ps
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
        Just voteId -> ss_sendRpc ss (r_candidateId rpc)
                    RequestVoteResponse
                    { r_term = r_term rpc
                    , r_voteGranted = voteId == r_candidateId rpc
                    , r_responderId = c_myServerId . ps_config $ ps
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
          ss_sendRpc ss (r_candidateId rpc)
            RequestVoteResponse
            { r_term = r_term rpc
            , r_voteGranted = candidateIsUpToDate
            , r_responderId = c_myServerId . ps_config $ ps
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
-- Debugging.
----------------------------------------------------------------
--
-- Can toggle debugging on and off in the config (later).

debug :: ServerState cmd -> String -> IO ()
debug ss msg = do
  ps <- CCM.readMVar (ss_persistentState ss)
  ss_debug ss $
    printf "Server %i: %s\n" (c_myServerId . ps_config $ ps) msg

data ControllerState cmd
  = ControllerState
    { cs_sidToSs :: !(Map ServerId (ServerState cmd))
    , cs_networkQueue :: !(Chan (NetworkEvent cmd))
    , cs_printfLock :: !(MVar ())
    }

debugController :: ControllerState cmd -> String -> IO ()
debugController cs msg = do
  printfConcurrent (cs_printfLock cs) $ printf "Controller: %s\n" msg

-- | Print to console without garbling the output.
--
-- The lock is used to avoid two callers writing at the same time.
printfConcurrent :: MVar () -> String -> IO ()
printfConcurrent lock msg =
  CCM.withMVar lock (const $ printf msg)

----------------------------------------------------------------
-- Startup / initialization.
----------------------------------------------------------------

-- | Run some example servers.
main :: IO ()
main = do
  startServers (Proxy :: Proxy DummyCmd) 3 dummyInitialSmState

-- | A command type for which there is no state-machine state.
data DummyCmd = DummyCmd deriving ( Show )
type instance SmState DummyCmd = ()

dummyInitialSmState :: SmState DummyCmd
dummyInitialSmState = ()

----------------------------------------------------------------

startServers ::
  forall cmd.
  Show cmd =>
  Proxy cmd -> Integer -> SmState cmd -> IO ()
startServers _ numServers initialSmState = do
  (networkQueue :: Chan (NetworkEvent cmd)) <- CCC.newChan
  printfLock <- CCM.newMVar ()
  sss <- CM.forM serverIds $ \myId -> do
    let config = Config
                 { c_otherServerIds = DL.delete myId serverIds
                 , c_myServerId = myId
                 }
    mkInitialServerState config initialSmState printfLock networkQueue
  threadPairs <- CM.mapM (startServer printfLock) sss
  let sidToSs = Map.fromList $ zip serverIds sss
  let cs = ControllerState
           { cs_sidToSs = sidToSs
           , cs_networkQueue = networkQueue
           , cs_printfLock = printfLock
           }
  controlLoop cs `CES.finally` do
    CM.forM_ threadPairs $ \(mainThread, smThread) -> do
      CCA.cancel mainThread
      CCA.cancel smThread
  where
    serverIds = [ 1 .. numServers ]

-- | Read "network" events off the queue and deliver them.
controlLoop :: Show cmd => ControllerState cmd -> IO ()
controlLoop cs = CM.forever $ do
  nwe <- CCC.readChan (cs_networkQueue cs)
  debugController cs $ show nwe
  case nwe of
    SendRpc sid rpc -> do
      let Just ss = Map.lookup sid (cs_sidToSs cs)
      CCC.writeChan (ss_mainQueue ss) $ Rpc rpc

startServer :: MVar () -> ServerState cmd -> IO (Async (), Async ())
startServer lock ss = do
  ps <- CCM.readMVar (ss_persistentState ss)
  let myId = c_myServerId . ps_config $ ps
  mainThread <- CCA.async $ do
    becomeFollower ss 0
    mainLoop ss `CES.finally` do
      cancelTimer ss
      printfConcurrent lock $
        printf "Server %i: mainLoop exiting!\n" myId
  smThread <- CCA.async $ do
    smLoop ss `CES.finally` do
      printfConcurrent lock $
        printf "Server %i: smLoop exiting!\n" myId
  return (mainThread, smThread)

-- | Initialize and return a 'ServerState'.
mkInitialServerState ::
  Config -> SmState cmd -> MVar () -> Chan (NetworkEvent cmd) ->
  IO (ServerState cmd)
mkInitialServerState config smState printfLock networkQueue = do
  ss_timer <- CCM.newMVar =<< CCA.async (return ())
  ss_mainQueue <- CCC.newChan
  ss_smQueue <- CCC.newChan
  ss_role <- CCM.newMVar Follower
  ss_persistentState <- CCM.newMVar persistentState
  ss_volatileState <- CCM.newMVar volatileState
  ss_volatileLeaderState <- CCM.newMVar Nothing
  ss_volatileCandidateState <- CCM.newMVar Nothing
  let ss_sendRpc sid rpc = CCC.writeChan networkQueue (SendRpc sid rpc)
  let ss_debug msg = printfConcurrent printfLock msg
  return ServerState{..}
  where
    persistentState =
      PersistentState
      { ps_currentTerm = 0
      , ps_votedFor = Nothing
      -- Initialize the log with a dummy event, so that we don't have
      -- to have a special case for empty logs. This also allows us to
      -- use log indices to index into the log list.
      , ps_log = [ LogEntry 0 0 NoOp ]
      , ps_config = config
      }
    volatileState =
      VolatileState
      { vs_commitIndex = 0
      , vs_lastApplied = 0
      , vs_smState = smState
      }

----------------------------------------------------------------
-- * Types
----------------------------------------------------------------

-- | Use a @newtype@ to decrease the chance of accidentally
-- interchanging a 'Term' and an 'Index', since they are both
-- 'Integer' and used in similar places.
newtype Term = Term Integer
  deriving ( Eq, Num, Ord, Show )
-- | A separate projection function, so that it doesn't clutter the
-- derived 'Show' instance.
unTerm :: Term -> Integer
unTerm (Term x) = x

-- | Treat this opaquely, so I can change it later, e.g. to add
-- networking. Or, could always use integers to represent server ids,
-- and have a separate abstraction that contacts servers using their
-- ids. This might be better! E.g.,
--
-- > send :: ServerId -> RPC -> m ()
type ServerId = Integer
-- | See 'Term' above.
newtype Index = Index Integer
  deriving ( Eq, Num, Ord, Show )
unIndex (Index x) = x

-- | Log entries are parameterized by the command type @cmd@. E.g.,
-- for a key-value store service as used in the paper, the command
-- type could be
--
-- > data KvCmd = Get Key | Put Key Value
data LogEntry cmd =
  LogEntry
  { le_index :: !Index
  , le_term :: !Term
  , le_cmd :: !(Command cmd)
  }
  deriving ( Show )

data Command cmd
    -- We initialize the logs with a no-op event. Also, new leaders
    -- can use no-op events when they want to increase their commit
    -- index, but have no committed commands from their term; see
    -- Section 5.4.2.
  = NoOp
  | Command cmd
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
    , r_nextLogIndex :: !Index
      -- ^ Not in paper. In successful responses, this is the index of
      -- the next log entry the leader should try to send us. In
      -- unsuccessful responses, this field is set to the index before
      -- `r_lastLogIndex` in the rpc, but but could be used to
      -- implement an optimization; see comments in
      -- 'handleAppendEntriesForCurrentTermAsFollower'.
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
    , c_myServerId :: !ServerId
    }
  deriving ( Show )

-- | Event for the main queue.
data MainEvent cmd
  = Rpc (Rpc cmd)
  | StartElection
  deriving ( Show )

-- | Event for the state machine queue.
data SmEvent
  = UpdateSm
  deriving ( Show )

-- | Event for the controller thread network simulation queue.
data NetworkEvent cmd
  = SendRpc !ServerId !(Rpc cmd)
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
    , ss_smQueue :: !(Chan SmEvent)
      -- ^ State machine event queue.
    , ss_role :: !(MVar ServerRole)
    , ss_persistentState :: !(MVar (PersistentState cmd))
    , ss_volatileState :: !(MVar (VolatileState cmd))
    , ss_volatileLeaderState :: !(MVar (Maybe VolatileLeaderState))
    , ss_volatileCandidateState :: !(MVar (Maybe VolatileCandidateState))
    , ss_sendRpc :: !(ServerId -> Rpc cmd -> IO ())
    , ss_debug :: !(String -> IO ())
    }
