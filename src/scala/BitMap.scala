class BitMap(row: Int = 100) {
  var array = new Array[Int](row)

  def getMap = array

  def setMap(array: Array[Int]) = this.array = array

  def show = {
    array.foreach(int => {
      for (i <- 0 to 31) {
        print(int >> i & 1)
      }
      println()
    })
  }


  def setBit(i: Int) {
    if (i >> 5 < row) {
      array(i >> 5) |= (1 << (i & 0X1F))
    } else {
      println("图长度不足，无法统计")
    }
  }

  @deprecated
  def reset(i: Int) {
    array(i >> 5) &= (~(1 << (i & 0X1F)))
  }

  def exists(i: Int): Boolean = {
    if (i >> 5 < row) {
      (array(i >> 5) & (1 << (i & 0X1F))) != 0
    } else {
      false
    }
  }


  def join(brray: Array[Int]) = {
    if (array.length < brray.length) {
      (for (index <- 0 until array.length) yield array(index) | (brray(index))) ++: (brray.takeRight(brray.length - array.length))


    } else if ((array.length > brray.length)) {
      (for (index <- 0 until brray.length) yield array(index) | (brray(index))) ++: (array.takeRight(array.length - brray.length))

    } else {
      (for (index <- 0 until array.length) yield array(index) | (brray(index))).toArray
    }
  }

  def join(bitMap: BitMap): Array[Int] = {
    join(bitMap.getMap)
  }

  def count = {
    array.map(_.toBinaryString.count(bit => bit == '1')).reduce(_ + _)
  }


}

object BitMap extends Serializable {

  def creat(array: Array[Int]) = {
    val bitMap = new BitMap()
    bitMap.setMap(array)
    bitMap
  }

  def setBit(array: Array[Int], i: Int) {
    if (i >> 5 < array.length) {
      array(i >> 5) |= (1 << (i & 0X1F))
    } else {
      println("图长度不足，无法统计")
    }
  }

  def exists(array: Array[Int], i: Int): Boolean = {
    if (i >> 5 < array.length) {
      (array(i >> 5) & (1 << (i & 0X1F))) != 0
    } else {
      false
    }
  }

  def count(array: Array[Int]): Int = {
    array.map(_.toBinaryString.count(bit => bit == '1')).reduce(_ + _)
  }

  def join(array: Array[Int], brray: Array[Int]) = {
    if (array.length < brray.length) {
      (for (index <- 0 until array.length) yield array(index) | (brray(index))) ++: (brray.takeRight(brray.length - array.length))


    } else if ((array.length > brray.length)) {
      (for (index <- 0 until brray.length) yield array(index) | (brray(index))) ++: (array.takeRight(array.length - brray.length))

    } else {
      (for (index <- 0 until array.length) yield array(index) | (brray(index))).toArray
    }
  }

  def show(array: Array[Int]) = {
    array.foreach(int => {
      for (i <- 0 to 31) {
        print(int >> i & 1)
      }
      println()
    })
  }
}


