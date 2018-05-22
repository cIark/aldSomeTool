package BitmapUserCount

case class BitMap(row: Int = 10000) {
  var array: Array[Int] = new Array[Int](row)
  def setMap(bitmap: Array[Int]) = array = bitmap
  def getMap: Array[Int]=array
  def show={
    array.foreach(int=>{
      for(i<- 0 to 31){print(int>>i&1)}
      println()
    })
  }
  def setBit(i: Int) {
    if (i >> 5 <= row) {
      array(i >> 5) |= (1 << (i & 0X1F))
    }else{
      println("图长度不足，无法统计")
    }
  }
  def reset(i: Int) {
    array(i >> 5) &= (~(1 << (i & 0X1F)))
  }
  def exists(i: Int): Boolean = {
    (array(i >> 5) & (1 << (i & 0X1F))) != 0
  }

}


