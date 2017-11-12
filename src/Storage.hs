module Storage
  (
    initStorage
  , Storage
  , get
  , set
  , delete
  , increment
  , decrement
  ) where

import Query
import Serialization
import Control.Monad.STM
import qualified STMContainers.Map as Map

type Storage = Map.Map Key Value

initStorage :: IO Storage
initStorage = Map.newIO

get :: Storage -> Key -> IO (Maybe Value)
get storage key = atomically $ Map.lookup key storage

set :: Storage -> Key -> Value -> IO ()
set storage key value = atomically $ Map.insert value key storage

delete :: Storage -> Key -> IO ()
delete storage key = atomically $ Map.delete key storage

increment :: Storage -> Key -> Integer -> IO Bool
increment storage key amount = atomically $ updateInteger storage key (+amount)

decrement :: Storage -> Key -> Integer -> IO Bool
decrement storage key amount = atomically $ updateInteger storage key (subtract amount)

updateInteger :: Storage -> Key -> (Integer -> Integer) -> STM Bool
updateInteger storage key f = do
    value <- Map.lookup key storage
    case value >>= readInteger of
      Nothing -> return False
      Just int -> do
        Map.insert (writeInteger $ f int) key storage
        return True
