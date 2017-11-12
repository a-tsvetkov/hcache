module Serialization
 (
   readInteger
 , writeInteger
 ) where

import Data.ByteString.Builder
import Data.ByteString.Lazy (toStrict)
import qualified Data.ByteString.Char8 as ByteString

readInteger :: ByteString.ByteString -> Maybe Integer
readInteger s = fst <$> ByteString.readInteger s

writeInteger :: Integer -> ByteString.ByteString
writeInteger int = toStrict $ toLazyByteString $ integerDec int
