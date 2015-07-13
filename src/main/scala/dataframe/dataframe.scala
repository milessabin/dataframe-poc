package dataframe

import shapeless._, ops.hlist._

object Dataframe {

  trait DataType {
    def asDouble(value: Any): Double
  }

  object DataTypes {
    val int32: DataType =
      new DataType {
        def asDouble(value: Any): Double = value match {
          case i: Int => i.toDouble
          case d: Double => d
          case s: String => s.toDouble
        }
      }

    val float64: DataType =
      new DataType {
        def asDouble(value: Any): Double = value match {
          case i: Int => i.toDouble
          case d: Double => d
          case s: String => s.toDouble
        }
      }
  }

  case class Column(idx: Int, dataType: DataType)

  trait Aggregator {
    type Elem
    type Acc

    def zero: Acc

    def mapFunc(columnValue: Any, columnType: DataType): Elem

    def foldFunc(input: Elem, currOutput: Acc): Acc
  }

  object Aggregator {
    type Aux[Elem0, Acc0] = Aggregator { type Elem = Elem0; type Acc = Acc0 }
  }

  case object SumAggregatorDouble extends Aggregator {
    override type Elem = Double
    override type Acc = Double

    override def zero: Acc = 0d

    override def mapFunc(columnValue: Any, columnType: DataType): Elem = columnType.asDouble(columnValue)

    override def foldFunc(input: Elem, currOutput: Acc): Acc = currOutput + input
  }


  case object SumAggregatorInt extends Aggregator {
    override type Acc = Int
    override type Elem = Int

    override def zero: Acc = 0

    override def mapFunc(columnValue: Any, columnType: DataType): Elem = columnType.asDouble(columnValue).toInt

    override def foldFunc(input: Elem, currOutput: Acc): Acc = currOutput + input
  }

  case class ColumnAggregator[In, Out](column: Column, aggregator: Aggregator.Aux[In, Out])

  trait FoldCols[C <: HList] {
    type In <: HList
    type Out <: HList
    def apply(c: C, rows: List[In], acc: Out): Out
  }

  object FoldCols {
    type Aux[C <: HList, In0 <: HList, Out0 <: HList] = FoldCols[C] { type In = In0 ; type Out = Out0 }
    def apply[C <: HList, In <: HList, Out <: HList](c: C, rows: List[In], acc: Out)
      (implicit fc: FoldCols.Aux[C, In, Out]) = fc(c, rows, acc)

    implicit def foldHNil: Aux[HNil, HNil, HNil] =
      new FoldCols[HNil] {
        type In = HNil
        type Out = HNil
        def apply(c: HNil, rows: List[HNil], acc: HNil) = HNil
      }

    implicit def foldHCons[InH, OutH, T <: HList, InT <: HList, OutT <: HList]
      (implicit ft: FoldCols.Aux[T, InT, OutT]): Aux[ColumnAggregator[InH, OutH] :: T, InH :: InT, OutH :: OutT] =
        new FoldCols[ColumnAggregator[InH, OutH] :: T] {
          type In = InH :: ft.In
          type Out = OutH :: ft.Out
          def apply(c: ColumnAggregator[InH, OutH] :: T, rows: List[InH :: InT], acc: OutH :: OutT): Out = {
            val aggregator = c.head.aggregator
            val res =
              rows.foldLeft(acc.head) { case (acc, row) =>
                val x = row.head
                aggregator.foldFunc(x, acc)
              }

            res :: ft(c.tail, rows.map(_.tail), acc.tail)
          }
        }
  }

  trait AggregateCols[C <: HList] {
    type Out <: HList
    def apply(c: C, row: List[Any]): Out
  }

  object AggregateCols {
    type Aux[C <: HList, Out0 <: HList] = AggregateCols[C] { type Out = Out0 }
    def apply[C <: HList](c: C, row: List[Any])(implicit ac: AggregateCols[C]) = ac(c, row)

    implicit def aggregateHNil: Aux[HNil, HNil] =
      new AggregateCols[HNil] {
        type Out = HNil
        def apply(c: HNil, row: List[Any]) = HNil
      }

    implicit def aggregateHCons[InH, OutH, T <: HList, OT <: HList]
      (implicit at: AggregateCols.Aux[T, OT]): Aux[ColumnAggregator[InH, OutH] :: T, InH :: at.Out] =
        new AggregateCols[ColumnAggregator[InH, OutH] :: T] {
          type Out = InH :: at.Out
          def apply(c: ColumnAggregator[InH, OutH] :: T, row: List[Any]): Out = {
            val x = row(c.head.column.idx)
            val dataType = c.head.column.dataType
            c.head.aggregator.mapFunc(x, dataType) :: at(c.tail, row)
          }
        }
  }

  trait Zeros[C <: HList] {
    type Out <: HList
    def apply(c: C): Out
  }

  object Zeros {
    type Aux[C <: HList, Out0 <: HList] = Zeros[C] { type Out = Out0 }
    implicit def hnilZeros: Aux[HNil, HNil] =
      new Zeros[HNil] {
        type Out = HNil
        def apply(c: HNil) = HNil
      }

    implicit def hconsZeros[InH, OutH, T <: HList, OutT <: HList]
      (implicit zt: Zeros.Aux[T, OutT]): Aux[ColumnAggregator[InH, OutH] :: T, OutH :: OutT] =
        new Zeros[ColumnAggregator[InH, OutH] :: T] {
          type Out = OutH :: OutT
          def apply(c: ColumnAggregator[InH, OutH] :: T) =
            c.head.aggregator.zero :: zt(c.tail)
        }
  }

  case class GroupByAggregator[C <: HList, In <: HList, Out <: HList]
    (rows: List[List[Any]], colAggregators: C)
    (implicit
       zeros: Zeros.Aux[C, Out],
       ac: AggregateCols.Aux[C, In],
       fc: FoldCols.Aux[C, In, Out]
    ) {

    def mapByKey(): List[In] = {
      rows.map { row =>
        AggregateCols(colAggregators, row)
      }
    }

    def foldByKey(mapResults: List[In]): Out = {
      val zeroValues: Out = zeros(colAggregators)

      FoldCols(colAggregators, mapResults, zeroValues).asInstanceOf[Out]
    }
  }
}

object Test extends App {
  import Dataframe._
  val inputRows = List(
    List[Any](1, 1d),
    List[Any](2, 1d),
    List[Any](2, 1d)
  )

  val columnAggregator =
    ColumnAggregator(Column(0, DataTypes.int32), SumAggregatorInt) ::
    ColumnAggregator(Column(1, DataTypes.float64), SumAggregatorDouble) ::
    HNil

  val groupByAggregator = GroupByAggregator(inputRows, columnAggregator)

  val mapResults =groupByAggregator.mapByKey()
  println(mapResults)

  val foldResults = groupByAggregator.foldByKey(mapResults)
  println(foldResults)
}
