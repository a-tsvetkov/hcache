module Query
  (
    parseQuery
  , Key
  , Value
  , Query(..)
  ) where

import Control.Monad.State.Lazy
import Data.Char (toUpper, isSpace)
import qualified Data.ByteString.Char8 as ByteString

type Key = ByteString.ByteString
type Value = ByteString.ByteString

data Query = Get Key | Set Key Value | Delete Key | Incr Key | Decr Key deriving (Show)

parseQuery :: ByteString.ByteString -> Maybe Query
parseQuery qBStr = do
  splitted <- splitCommand qBStr
  case splitted of
    ("SET", (key:value:[])) -> return (Set key value)
    ("GET", (key:[])) -> return (Get key)
    ("DELETE", (key:[])) -> return (Delete key)
    ("INCR", (key:[])) -> return (Incr key)
    ("DECR", (key:[])) -> return (Decr key)
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
            Nothing -> return []
            Just i -> do
              arg <- state $ ByteString.splitAt i
              when quoted $ modify $ ByteString.drop 1
              (arg:) <$> splitQuotes

    quotes = ['\'', '"']
