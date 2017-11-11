module Storage
  (
    initStorage
  , Storage
  , get
  , set
  , delete
  ) where

import Query
import Control.Monad.STM
import qualified Data.ByteString as ByteString
import qualified STMContainers.Map as Map

type Storage = Map.Map ByteString.ByteString ByteString.ByteString

initStorage :: IO Storage
initStorage = Map.newIO

get :: Storage -> Key -> IO (Maybe ByteString.ByteString)
get storage key = atomically $ Map.lookup key storage

set :: Storage -> Key -> Value -> IO ()
set storage key value = atomically $ Map.insert value key storage

delete :: Storage -> Key -> IO ()
delete storage key = atomically $ Map.delete key storage
