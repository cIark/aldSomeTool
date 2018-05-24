
object TestBitMap {
  def main(args: Array[String]): Unit = {
    val bitMap= BitMap(10)
    bitMap.setBit(0)
    bitMap.setBit(1)
    bitMap.setBit(2)
    bitMap.setBit(2)
    bitMap.setBit(32)
    bitMap.setBit(3)
    bitMap.setBit(319)

bitMap.show
  }
}
