module Main where

import Data.Map ( Map )
import qualified Data.Map as Map

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
--         randomly chosen, but I don't remember if that's everytime,
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
--   loop that determines when to send heartbeats, that is reset on
--   for any other communication with corresponding follower. But to
--   start, can just send heartbeats regardless of whether they are
--   necessary and save the resets for later optimization. But for
--   followers and candidates we really do need an interuptable
--   election timer.
--
--   Properties:
--
--   - timer counts down and fires an event if it reaches zero. It
--     also restarts if it reaches zero.
--
--   - timer can be canceled / reset without firing its event.


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
