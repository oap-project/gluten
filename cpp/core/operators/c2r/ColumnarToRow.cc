/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "ColumnarToRow.h"

#include <arrow/util/decimal.h>

namespace gluten {

int64_t ColumnarToRowConverter::calculatedFixeSizePerRow(std::shared_ptr<arrow::Schema> schema, int64_t numCols) {
  std::vector<std::shared_ptr<arrow::Field>> fields = schema->fields();
  // Calculate the decimal col num when the precision >18
  int32_t count = 0;
  for (auto i = 0; i < numCols; i++) {
    auto type = fields[i]->type();
    if (type->id() == arrow::Decimal128Type::type_id) {
      auto dtype = dynamic_cast<arrow::Decimal128Type*>(type.get());
      int32_t precision = dtype->precision();
      if (precision > 18)
        count++;
    }
  }

  int64_t fixedSize = calculateBitSetWidthInBytes(numCols) + numCols * 8;
  int64_t decimalColsSize = count * 16;
  return fixedSize + decimalColsSize;
}

int32_t ColumnarToRowConverter::getNumberOfLeadingZeros(uint32_t i) {
  // TODO: we can get faster implementation by gcc build-in function
  // HD, Figure 5-6
  if (i == 0)
    return 32;
  int32_t n = 1;
  if (i >> 16 == 0) {
    n += 16;
    i <<= 16;
  }
  if (i >> 24 == 0) {
    n += 8;
    i <<= 8;
  }
  if (i >> 28 == 0) {
    n += 4;
    i <<= 4;
  }
  if (i >> 30 == 0) {
    n += 2;
    i <<= 2;
  }
  n -= i >> 31;
  return n;
}

int32_t ColumnarToRowConverter::getBitLength(int32_t sig, const std::vector<int32_t>& mag, int32_t len) {
  int32_t n = -1;
  if (len == 0) {
    n = 0;
  } else {
    // Calculate the bit length of the magnitude
    int32_t magBitLength = ((len - 1) << 5) + getBitLengthForInt((uint32_t)mag[0]);
    if (sig < 0) {
      // Check if magnitude is a power of two
      bool pow2 = (getBitCount((uint32_t)mag[0]) == 1);
      for (int i = 1; i < len && pow2; i++)
        pow2 = (mag[i] == 0);

      n = (pow2 ? magBitLength - 1 : magBitLength);
    } else {
      n = magBitLength;
    }
  }
  return n;
}

std::vector<uint32_t> ColumnarToRowConverter::convertMagArray(int64_t newHigh, uint64_t newLow, int32_t* size) {
  std::vector<uint32_t> mag;
  int64_t orignalLow = newLow;
  int64_t orignalHigh = newHigh;
  mag.push_back(newHigh >>= 32);
  mag.push_back((uint32_t)orignalHigh);
  mag.push_back(newLow >>= 32);
  mag.push_back((uint32_t)orignalLow);

  int32_t start = 0;
  // remove the front 0
  for (int32_t i = 0; i < 4; i++) {
    if (mag[i] == 0)
      start++;
    if (mag[i] != 0)
      break;
  }

  int32_t length = 4 - start;
  std::vector<uint32_t> newMag;
  // get the mag after remove the high 0
  for (int32_t i = start; i < 4; i++) {
    newMag.push_back(mag[i]);
  }

  *size = length;
  return newMag;
}

/*
 *  This method refer to the BigInterger#toByteArray() method in Java side.
 */
std::array<uint8_t, 16> ColumnarToRowConverter::toByteArray(arrow::Decimal128 value, int32_t* length) {
  arrow::Decimal128 newValue;
  int32_t sig;
  if (value > 0) {
    newValue = value;
    sig = 1;
  } else if (value < 0) {
    newValue = value.Abs();
    sig = -1;
  } else {
    newValue = value;
    sig = 0;
  }

  int64_t newHigh = newValue.high_bits();
  uint64_t newLow = newValue.low_bits();

  std::vector<uint32_t> mag;
  int32_t size;
  mag = convertMagArray(newHigh, newLow, &size);

  std::vector<int32_t> finalMag;
  for (auto i = 0; i < size; i++) {
    finalMag.push_back(mag[i]);
  }

  int32_t byteLength = getBitLength(sig, finalMag, size) / 8 + 1;

  std::array<uint8_t, 16> out{{0}};
  uint32_t nextInt = 0;
  for (int32_t i = byteLength - 1, bytesCopied = 4, intIndex = 0; i >= 0; i--) {
    if (bytesCopied == 4) {
      nextInt = getInt(intIndex++, sig, finalMag, size);
      bytesCopied = 1;
    } else {
      nextInt >>= 8;
      bytesCopied++;
    }

    out[i] = (uint8_t)nextInt;
  }
  *length = byteLength;
  return out;
}

} // namespace gluten
