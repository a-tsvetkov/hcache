module Serialization.SerializationSpec
  (
    main
  , spec
  ) where

import Test.Hspec
import qualified Data.ByteString.Char8 as B
import Serialization

main :: IO ()
main = hspec spec

spec :: Spec
spec = do
  describe "serialization" $ do
    context "readInteger" $ do
      it "should read an Integer values from a ByteString" $ do
        readInteger (B.pack "1234") `shouldBe` (Just 1234 :: Maybe Integer)

      it "should ignore string after int" $ do
        readInteger (B.pack "1234 567") `shouldBe` (Just 1234 :: Maybe Integer)

      it "should properly handle large numbers" $ do
        readInteger (B.pack "1922337203685477580810") `shouldBe` (Just 1922337203685477580810 :: Maybe Integer)

      it "should properly handle negative numbers" $ do
        readInteger (B.pack "-100") `shouldBe` (Just (-100) :: Maybe Integer)

      it "should return Nothing on non-numeric strings" $ do
        readInteger (B.pack "one hundred") `shouldBe` Nothing

      it "should return Nothing if number is prepended by chars" $ do
        readInteger (B.pack "one hundred63") `shouldBe` Nothing

    context "writeInteger" $ do
      it "should write an Integer to a ByteString" $ do
        writeInteger 1234 `shouldBe` B.pack "1234"

      it "should properly handle large numbers" $ do
        writeInteger 1922337203685477580810 `shouldBe` B.pack "1922337203685477580810"

      it "should properly handle negative numbers" $ do
        writeInteger (-100) `shouldBe` B.pack "-100"
