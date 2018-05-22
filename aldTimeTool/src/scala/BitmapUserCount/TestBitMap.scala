package BitmapUserCount

object TestBitMap {
  def main(args: Array[String]): Unit = {
    val bitMap= BitMap(40)
    bitMap.setBit(0)
    bitMap.setBit(1)
    bitMap.setBit(2)
    bitMap.setBit(2)
    bitMap.setBit(32)
    bitMap.setBit(3)
    bitMap.setBit(500)
    println(bitMap.exists(500))
    println(bitMap.exists(499))
    bitMap.show
  }
}
