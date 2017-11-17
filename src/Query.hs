module Query
  (
    parseQuery
  , Key
  , Value
  , Query(..)
  ) where

import           Serialization (readInteger)
import           Control.Monad.State.Lazy
import           Data.Char (toUpper, isSpace)
import qualified Data.ByteString.Char8 as ByteString

type Key = ByteString.ByteString
type Value = ByteString.ByteString

data Query = Get [Key]
  | Set Key Value
  | Delete Key
  | Incr Key Integer
  | Decr Key Integer
  | Add Key Value
  | Replace Key Value
  | Append Key Value
  | Prepend Key Value
  deriving (Show, Eq)

parseQuery :: ByteString.ByteString -> Maybe Query
parseQuery qBStr = do
  splitted <- splitCommand qBStr
  case splitted of
    ("SET", (key:value:[])) -> return (Set key value)
    ("ADD", (key:value:[])) -> return (Add key value)
    ("REPLACE", (key:value:[])) -> return (Replace key value)
    ("INCR", (key:value:[])) -> do
      parsed <- readInteger value
      return (Incr key parsed)
    ("DECR", (key:value:[])) -> do
      parsed <- readInteger value
      return (Decr key parsed)
    ("APPEND", (key:value:[])) -> return (Append key value)
    ("PREPEND", (key:value:[])) -> return (Prepend key value)
    ("GET", keys@(_:_)) -> return (Get keys)
    ("DELETE", (key:[])) -> return (Delete key)
    _ -> Nothing
  where
    splitCommand :: ByteString.ByteString -> Maybe (String, [ByteString.ByteString])
    splitCommand str =
      let splitted = evalState splitQuotes str
      in
        case splitted of
          (cmd:args) -> let command = map toUpper $ ByteString.unpack cmd in Just (command, args)
          _ -> Nothing

    splitQuotes :: State ByteString.ByteString [ByteString.ByteString]
    splitQuotes = do
      modify $ ByteString.dropWhile (isSpace)
      str <- get
      case ByteString.uncons str of
        Nothing -> return []
        Just (sep, t) -> do
          let quoted = (sep `elem` quotes)
              predicate = if quoted then (==sep) else isSpace
          when quoted $ put t
          end <- ByteString.findIndex predicate <$> get
          case end of
            Nothing ->
              if quoted
              then return []
              else do
                rest <- state $ ByteString.break predicate
                return [rest]
            Just i -> do
              arg <- state $ ByteString.splitAt i
              when quoted $ modify $ ByteString.drop 1
              (arg:) <$> splitQuotes

    quotes = ['\'', '"']
