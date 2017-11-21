module Data.HashTable.IO.ConcurrentLinearSpec
  (
    main
  , spec
  ) where

import           Test.Hspec
import           Control.Monad
import qualified Data.HashTable.IO.ConcurrentLinear as HT

main :: IO ()
main = hspec spec

spec :: Spec
spec = do
  describe "Data.HashTable.IO.ConcurrentLinear" $ do
    context "lookup" $ do
      it "should get value by key" $ do
        ht <- HT.new
        HT.insert ht "foo" "bar"
        res <- HT.lookup ht "foo"
        res `shouldBe` Just "bar"

      it "should get value when multiple values exist" $ do
        ht <- HT.new
        forM_ ['a'..'z'] (\c -> HT.insert ht ("key" ++ [c])  ("value" ++ [c]))
        res <- HT.lookup ht "keyp"
        res `shouldBe` Just "valuep"

    context "insert" $ do
      it "should create new value" $ do
        ht <- HT.new
        HT.insert ht "foo" "bar"
        res <- HT.lookup ht "foo"
        res `shouldBe` Just "bar"

      it "should overwrite existing value" $ do
        ht <- HT.new
        HT.insert ht "foo" "bar"
        HT.insert ht "foo" "barbarbar"
        res <- HT.lookup ht "foo"
        res `shouldBe` Just "barbarbar"

      it "should insert value when multiple values exist" $ do
        ht <- HT.new
        forM_ ['a'..'z'] (\c -> HT.insert ht ("key" ++ [c])  ("value" ++ [c]))
        HT.insert ht "foo" "bar"
        res <- HT.lookup ht "foo"
        res `shouldBe` Just "bar"

    context "delete" $ do
      it "should delete value" $ do
        ht <- HT.new
        HT.insert ht "foo" "bar"
        HT.delete ht "foo"
        res <- HT.lookup ht "foo"
        res `shouldBe` Nothing

      it "should not interact with other values" $ do
        ht <- HT.new
        forM_ ['a'..'z'] (\c -> HT.insert ht ("key" ++ [c])  ("value" ++ [c]))
        HT.delete ht "keyk"
        res <- HT.lookup ht "keyk"
        res `shouldBe` Nothing
        res' <- HT.lookup ht "keym"
        res' `shouldBe` Just "valuem"
