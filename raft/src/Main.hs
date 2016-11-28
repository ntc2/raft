module Main where

import qualified Control.Concurrent as CC
import           Control.Concurrent.Async ( Async )
import qualified Control.Concurrent.Async as CCA
import           Control.Concurrent.MVar ( MVar )
import qualified Control.Concurrent.MVar as CCM
import qualified Control.Monad as CM
import           Data.Map.Strict ( Map )
import qualified Data.Map.Strict as Map

main :: IO ()
main = do
  putStrLn "hello world"


----------------------------------------------------------------
-- * Client
----------------------------------------------------------------

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
--       - [ ] How is the heartbeat delta determined?
--
--
--   - Follower:
--
--     - Becomes candidate if heartbeat is not received during timeout.
--
--       - [ ] How is the heartbeat timeout determined? Iirc it's
--         randomly chosen, but I don't remember if that's every time,
--         or once per server (presumably every time ...).
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
--
-- Tasks
--
-- - [ ] Implement resetable timer loop. E.g., for the leader, want a
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

-- | Run a delayed action, returning a handle that can be used to
-- cancel the delayed action -- using 'CCA.cancel' -- if it has not
-- run yet.
--
-- - delay: time to delay in milliseconds.
-- - action: the action to run after the delay.
delayedAction :: Int -> IO () -> IO (Async ())
delayedAction delay action = do
  CCA.async $ CC.threadDelay delay >> action

-- | The election timeout is randomly chosen between 'minElectionTimeout'
-- and @2 * minElectionTimeout@, on each iteration of the election loop.
minElectionTimeout :: Int
minElectionTimeout = 150 -- Paper suggestion.

startElectionTimer :: ServerState cmd -> IO ()
startElectionTimer ss = do
  delay <- error "TODO: choose a random election timeout between T and 2*T!"
  timer <- delayedAction delay $ handleElectionTimeout ss
  CCM.putMVar (ss_timer ss) timer

restartElectionTimer :: ServerState cmd -> IO ()
restartElectionTimer ss = do
  CCA.cancel =<< CCM.takeMVar (ss_timer ss)
  startElectionTimer ss

handleElectionTimeout :: ServerState cmd -> IO ()
handleElectionTimeout ss = do
  role <- CCM.takeMVar $ ss_role ss
  case role of
    Leader -> error "The leader should not be running an election timer!"
    Follower -> startElection ss
    Candidate -> startElection ss

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
  CCM.putMVar (ss_role ss) Candidate
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
      -- Initialize the volatile candidate state and count our self
      -- vote. If we add more fields to the volatile candidate state,
      -- then create a separate initialization function.
      CM.void $ CCM.swapMVar (ss_volatileCandidateState ss)
        (Just $ VolatileCandidateState { vcs_numVotesReceived = 1 })
    requestVotes = undefined

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
data Entry cmd = Entry Index Term cmd
  deriving ( Show )

{-
data KvCmd = Get Key | Put Key Value
type KvEntry = Entry KvCmd
-}

data PersistentState cmd
  = PersistentState
    { ps_currentTerm :: !Term
    , ps_votedFor :: !ServerId
      -- Use something more efficient later.
    , ps_log :: ![Entry cmd]
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
    { vcs_numVotesReceived :: !Integer
    }
  deriving ( Show )

data Rpc cmd
  = AppendEntries
    { r_term :: !Term
    , r_leaderId :: !ServerId
    , r_prevLogIndex :: !Index
    , r_prevLogTerm :: !Term
    , r_entries :: ![Entry cmd]
    , r_leaderCommit :: !Index
    }
  | RequestVote
    { r_term :: !Term
    , r_candidateId :: !ServerId
    , r_lastLogIndex :: !Index
    , r_lastLogTerm :: !Term
    }
  deriving ( Show )

data RpcResponse
  = AppendEntriesResponse
    { rr_term :: !Term
    , rr_success :: !Bool
    }
  | RequestVoteResponse
    { rr_term :: !Term
    , rr_voteGranted :: !Bool
    }
  deriving ( Show )

data ServerRole
  = Candidate
  | Follower
  | Leader
  deriving ( Show )

-- | The configuration of the collection of servers.
--
-- The Raft paper describes a protocol for updating the
-- configuration. For now we're going to assume it's constant for the
-- life of the servers.
data Config
  = Config
    { sc_serverIds :: ![ServerId] -- ^ The set of all server ids.
    }
  deriving ( Show )

-- | All of the server state.
--
-- Need to add the volatile and persistent state here, the special
-- leader state, etc.
--
-- I'm threading the state manually below ...
data ServerState cmd
  = ServerState
      -- Expect contention on the timer.
    { ss_timer :: !(MVar (Async ()))
      -- Not sure about contention on the role yet.
    , ss_role :: !(MVar ServerRole)
    , ss_persistentState :: !(MVar (PersistentState cmd))
    , ss_volatileState :: !(MVar VolatileState)
    , ss_volatileLeaderState :: !(MVar (Maybe VolatileLeaderState))
    , ss_volatileCandidateState :: !(MVar (Maybe VolatileCandidateState))
    }
