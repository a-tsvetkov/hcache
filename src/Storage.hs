module Storage
  (
    initStorage
  , Storage
  , get
  , set
  ) where

import Control.Monad.STM
import qualified Data.ByteString as ByteString
import qualified STMContainers.Map as Map

type Storage = Map.Map ByteString.ByteString ByteString.ByteString

initStorage :: IO Storage
initStorage = Map.newIO

get :: Storage -> ByteString.ByteString -> IO (Maybe ByteString.ByteString)
get storage key = atomically $ Map.lookup key storage

set :: Storage -> ByteString.ByteString -> ByteString.ByteString -> IO ()
set storage key value = atomically $ Map.insert value key storage
