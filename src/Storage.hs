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
  , append
  , prepend
  ) where

import qualified Focus as Focus
import           Data.Maybe (catMaybes)
import qualified Data.ByteString.Char8 as ByteString
import           Control.Monad
import           Control.Monad.STM
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
add storage key value = atomically $ Map.focus (
  \v ->
    case v of
      Just _ -> return (False, Focus.Keep)
      Nothing -> do
        return (True, Focus.Replace value)
  ) key storage

replace :: Storage -> Key -> Value -> IO Bool
replace storage key value = atomically $ Map.focus (
  \v ->
    case v of
      Just _ -> do
        return (True, Focus.Replace value)
      Nothing -> return (False, Focus.Keep)
  ) key storage

delete :: Storage -> Key -> IO Bool
delete storage key = atomically $ do
  Map.focus (
    \value ->
      case value of
        Just _ -> do
          return (True, Focus.Remove)
        Nothing -> return (False, Focus.Keep)
    ) key storage

increment :: Storage -> Key -> Integer -> IO (Maybe ByteString.ByteString)
increment storage key amount = atomically $ updateInteger storage key (+amount)

decrement :: Storage -> Key -> Integer -> IO (Maybe ByteString.ByteString)
decrement storage key amount = atomically $ updateInteger storage key (subtract amount)

append :: Storage -> Key -> Value -> IO Bool
append storage key value = atomically $ updateValue storage key $ flip (ByteString.append) value

prepend :: Storage -> Key -> Value -> IO Bool
prepend storage key value = atomically $ updateValue storage key $ ByteString.append value

updateValue :: Storage -> Key -> (Value -> Value) -> STM Bool
updateValue storage key f = Map.focus (
  \value ->
    case value of
      Nothing -> return (False, Focus.Keep)
      Just v -> do
        return (True, Focus.Replace $ f v)
    ) key storage

updateInteger :: Storage -> Key -> (Integer -> Integer) -> STM (Maybe ByteString.ByteString)
updateInteger storage key f = Map.focus (
  \value ->
    case value >>= readInteger of
      Nothing -> return (Nothing, Focus.Keep)
      Just int -> do
        let newValue = writeInteger $ f int
        return (Just newValue, Focus.Replace $ newValue)
  ) key storage
