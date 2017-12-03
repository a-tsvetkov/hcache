module Data.IORef.MarkedSpec
  (
    main
  , spec
  ) where

import Test.Hspec
import Data.IORef.Marked

main :: IO ()
main = hspec spec

spec :: Spec
spec = do
  describe "Marked IORef" $ do
    context "newIORef" $ do
      it "should create new ref with value" $ do
        r <- newIORef "foobar"
        v <- readIORef r
        v `shouldBe` "foobar"

    context "readIORef" $ do
      it "should read the value" $ do
        r <- newIORef "foobar"
        v <- readIORef r
        v `shouldBe` "foobar"

    context "writeIORef" $ do
      it "should update the value" $ do
        r <- newIORef "foo"
        writeIORef r "bar"
        v <- readIORef r
        v `shouldBe` "bar"

    context "readForCAS" $ do
      it "should return ticket to value" $ do
        r <- newIORef "foobar"
        t <- readForCAS r
        (peekTicket t) `shouldBe` "foobar"

    context "casIORef" $ do
      it "should succeed if value didn't change" $ do
        r <- newIORef "foo"
        t <- readForCAS r
        (success, t') <- casIORef r t "bar"
        success `shouldBe` True
        (peekTicket t') `shouldBe` "bar"

      it "should fail if value changed" $ do
        r <- newIORef "foo"
        t <- readForCAS r
        writeIORef r "foo1"
        (success, t') <- casIORef r t "bar"
        success `shouldBe` False
        (peekTicket t') `shouldBe` "foo1"

      it "should fail if ref has been marked" $ do
        r <- newIORef "foo"
        t <- readForCAS r
        markIORef r
        (success, t') <- casIORef r t "bar"
        success `shouldBe` False
        (peekTicket t') `shouldBe` "foo"

    context "atomicModifyIORefCAS" $ do
      it "should update and return value" $ do
        r <- newIORef "foo"
        res <- atomicModifyIORefCAS r (\v -> ("bar", v))
        val <- readIORef r
        res `shouldBe` "foo"
        val `shouldBe` "bar"

    context "atomicModifyIORefCAS_" $ do
      it "should update value" $ do
        r <- newIORef "foo"
        atomicModifyIORefCAS_ r (\_ -> "bar")
        val <- readIORef r
        val `shouldBe` "bar"
