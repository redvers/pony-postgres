primitive Int4
  fun binary(value: Array[U8] val): I32 =>
    var result: I32 = I32(0)
    for i in value.values() do
      result = (result << 8) + i.i32()
    end
    result
