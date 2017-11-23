module Data.HashTable.IO.ConcurrentLinearSpec
  (
    main
  , spec
  ) where

import           Test.Hspec
import           Data.List
import           Control.Monad
import qualified Data.HashTable.IO.ConcurrentLinear as HT
import           Focus (Decision(..))

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

    context "focus" $ do
      it "should supply value to the function" $ do
        ht <- HT.new
        HT.insert ht "foo" "bar"
        resFocus <- HT.focus ht "foo" (\v -> (v, Keep))
        resFocus `shouldBe` Just "bar"

      it "should supply Nothing if value doesnt exist" $ do
        ht <- HT.new :: IO (HT.HashTable String String)
        resFocus <- HT.focus ht "foo" (\v -> (v, Keep))
        resFocus `shouldBe` Nothing

      it "should keep value" $ do
        ht <- HT.new
        HT.insert ht "foo" "bar"
        resFocus <- HT.focus ht "foo" (\_ -> ("barbar", Keep))
        resLookup <- HT.lookup ht "foo"
        resFocus `shouldBe` "barbar"
        resLookup `shouldBe` Just "bar"

      it "should delete value" $ do
        ht <- HT.new
        HT.insert ht "foo" "bar"
        resFocus <- HT.focus ht "foo" (\_ -> ("barbar", Remove))
        resLookup <- HT.lookup ht "foo"
        resFocus `shouldBe` "barbar"
        resLookup `shouldBe` Nothing

      it "should update value" $ do
        ht <- HT.new
        HT.insert ht "foo" "bar"
        resFocus <- HT.focus ht "foo" (\_ -> ("barbar", Replace "barbarbar"))
        resLookup <- HT.lookup ht "foo"
        resFocus `shouldBe` "barbar"
        resLookup `shouldBe` Just "barbarbar"

      it "should create new value" $ do
        ht <- HT.new
        resFocus <- HT.focus ht "foo" (\_ -> ("barbar", Replace "barbarbar"))
        resLookup <- HT.lookup ht "foo"
        resFocus `shouldBe` "barbar"
        resLookup `shouldBe` Just "barbarbar"

    context "newSized" $ do
      it "create new sized storage" $ do
        ht <- HT.newSized 26
        forM_ ['a'..'z'] (\c -> HT.insert ht ("key" ++ [c])  ("value" ++ [c]))
        res <- HT.lookup ht "keyo"
        res `shouldBe` Just "valueo"

    context "resize" $ do
      it "should preserve all the values" $ do
        let assocs = map (\c -> ("key" ++ [c], "value" ++ [c])) $ take 32 ['0'..'z']
        ht <- HT.new
        forM_ assocs $ uncurry (HT.insert ht)
        forM_ assocs (
          \(k, v) -> do
            val <- HT.lookup ht k
            val `shouldBe` Just v
          )

      it "should not proce value duplicates" $ do
        let assocs = map (\c -> ("key" ++ [c], "value" ++ [c])) $ take 32 ['0'..'z']
        ht <- HT.new
        forM_ assocs $ uncurry (HT.insert ht)
        gotAssocs <- HT.assocs ht
        length gotAssocs `shouldBe` 32
        sort gotAssocs `shouldBe` assocs

    context "rehash" $ do
      it "should preserve all the values" $ do
        let assocs = map (\c -> ("key" ++ [c], "value" ++ [c])) $ take 64 ['0'..'z']
        ht <- HT.newSized 64
        forM_ assocs $ uncurry (HT.insert ht)
        forM_ assocs (
          \(k, v) -> do
            val <- HT.lookup ht k
            val `shouldBe` Just v
          )

      it "should not proce value duplicates" $ do
        let assocs = map (\c -> ("key" ++ [c], "value" ++ [c])) $ take 64 ['0'..'z']
        ht <- HT.new
        forM_ assocs $ uncurry (HT.insert ht)
        gotAssocs <- HT.assocs ht
        length gotAssocs `shouldBe` 64
        sort gotAssocs `shouldBe` assocs
