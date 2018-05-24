
object TestBitMap {
  def main(args: Array[String]): Unit = {
    val bitMap = new BitMap(4)
    bitMap.setBit(0)
    bitMap.setBit(1)
    bitMap.setBit(2)
    bitMap.setBit(115)
    BitMap.setBit(bitMap.getMap, 3)
    BitMap.show(bitMap.getMap)
    bitMap.show
    val result = BitMap.count(bitMap.getMap)
    println(result)
  }
}
