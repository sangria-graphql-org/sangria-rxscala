package sangria.streaming

import java.util.concurrent.atomic.AtomicInteger

import rx.lang.scala.Observable
import rx.lang.scala.subjects.PublishSubject
import sangria.streaming.rxscala.ObservableSubscriptionStream

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.postfixOps
import scala.concurrent.ExecutionContext.Implicits.global
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class RxScalaIntegrationSpec extends AnyWordSpec with Matchers {
  val impl: SubscriptionStream[Observable] = new ObservableSubscriptionStream

  "RxScala Integration" should {
    "support itself" in {
      impl.supported(rxscala.observableSubscriptionStream) should be (true)
    }

    "map" in {
      res(impl.map(Observable.from(List(1, 2, 10)))(_ + 1)) should be (List(2, 3, 11))
    }

    "singleFuture" in {
      res(impl.singleFuture(Future.successful("foo"))) should be (List("foo"))
    }

    "single" in {
      res(impl.single("foo")) should be (List("foo"))
    }

    "mapFuture" in {
      res(impl.mapFuture(Observable.from(List(1, 2, 10)))(x => Future.successful(x + 1))).toSet should be (Set(2, 3, 11))
    }

    "first" in {
      res(impl.first(Observable.from(List(1, 2, 3)))) should be (1)
    }

    "first throws error on error" in {
      an [IllegalStateException] should be thrownBy res(impl.first(Observable.error(new IllegalStateException())))
    }

    "first throws error on empty" in {
      an [IllegalStateException] should be thrownBy res(impl.first(Observable.from(Nil)))
    }

    "failed" in {
      an [IllegalStateException] should be thrownBy res(impl.failed(new IllegalStateException("foo")))
    }

    "onComplete handles success" in {
      val subj = PublishSubject[Int]()
      val count = new AtomicInteger(0)
      def inc() = count.getAndIncrement()

      val updated = impl.onComplete(subj)(inc())

      subj.onNext(1)
      subj.onNext(2)
      subj.onCompleted()

      updated.toBlocking.toList

      count.get() should be (1)
    }

    "onComplete handles failure" in {
      val subj = PublishSubject[Int]()
      val count = new AtomicInteger(0)
      def inc() = count.getAndIncrement()

      val updated = impl.onComplete(subj)(inc())

      subj.onError(new IllegalStateException("foo"))

      an [IllegalStateException] should be thrownBy updated.toBlocking.toList

      count.get() should be (1)
    }

    "flatMapFuture" in {
      res(impl.flatMapFuture(Future.successful(1))(i => Observable.from(List(i.toString, (i + 1).toString)))) should be (List("1", "2"))
    }

    "recover" in {
      val obs = Observable.from(List(1, 2, 3, 4)) map { i =>
        if (i == 3) throw new IllegalStateException("foo")
        else i
      }

      res(impl.recover(obs)(_ => 100)) should be (List(1, 2, 100))
    }

    "merge" in {
      val obs1 = Observable.from(List(1, 2))
      val obs2 = Observable.from(List(3, 4))
      val obs3 = Observable.from(List(100, 200))

      val result = res(impl.merge(Vector(obs1, obs2, obs3)))

      result should (
        have(size(6)) and
        contain(1) and
        contain(2) and
        contain(3) and
        contain(4) and
        contain(100) and
        contain(200))
    }

    "merge 2" in {
      val obs1 = Observable.from(List(1, 2))
      val obs2 = Observable.from(List(100, 200))

      val result = res(impl.merge(Vector(obs1, obs2)))

      result should (
        have(size(4)) and
        contain(1) and
        contain(2) and
        contain(100) and
        contain(200))
    }

    "merge 1" in {
      val obs1 = Observable.from(List(1, 2))

      val result = res(impl.merge(Vector(obs1)))

      result should (
        have(size(2)) and
        contain(1) and
        contain(2))
    }

    "merge throws exception on empty" in {
      an [IllegalStateException] should be thrownBy impl.merge(Vector.empty)
    }
  }

  def res[T](obs: Observable[T]) =
    obs.toBlocking.toList

  def res[T](f: Future[T]) =
    Await.result(f, 2 seconds)
}
