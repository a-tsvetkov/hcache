module Storage
  (
    initStorage
  , Storage
  , get
  , set
  , delete
  , increment
  , decrement
  , add
  , replace
  ) where

import Data.Maybe (catMaybes)
import Control.Monad
import Control.Monad.STM
import qualified STMContainers.Map as Map

import Query
import Serialization

type Storage = Map.Map Key Value

initStorage :: IO Storage
initStorage = Map.newIO

get :: Storage -> [Key] -> IO [(Key, Value)]
get storage keys = atomically $ catMaybes <$> forM keys (
  \key -> do
    result <- Map.lookup key storage
    return (result >>= (\v -> return (key, v)))
  )

set :: Storage -> Key -> Value -> IO ()
set storage key value = atomically $ Map.insert value key storage

add :: Storage -> Key -> Value -> IO Bool
add storage key value = atomically $ withValue storage key (
  \v ->
    case v of
      Just _ -> return False
      Nothing -> do
        Map.insert value key storage
        return True
  )

replace :: Storage -> Key -> Value -> IO Bool
replace storage key value = atomically $ withValue storage key (
  \v ->
    case v of
      Just _ -> do
        Map.insert value key storage
        return True
      Nothing -> return False
  )

delete :: Storage -> Key -> IO ()
delete storage key = atomically $ Map.delete key storage

increment :: Storage -> Key -> Integer -> IO Bool
increment storage key amount = atomically $ updateInteger storage key (+amount)

decrement :: Storage -> Key -> Integer -> IO Bool
decrement storage key amount = atomically $ updateInteger storage key (subtract amount)

withValue :: Storage -> Key -> (Maybe Value -> STM a) -> STM a
withValue storage key f = Map.lookup key storage >>= f

updateInteger :: Storage -> Key -> (Integer -> Integer) -> STM Bool
updateInteger storage key f = withValue storage key (
  \value ->
    case value >>= readInteger of
      Nothing -> return False
      Just int -> do
        Map.insert (writeInteger $ f int) key storage
        return True
  )