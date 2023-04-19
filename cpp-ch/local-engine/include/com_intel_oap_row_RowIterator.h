/* DO NOT EDIT THIS FILE - it is machine generated */
#include <jni.h>
/* Header for class com_intel_oap_row_RowIterator */

#ifndef _Included_com_intel_oap_row_RowIterator
#define _Included_com_intel_oap_row_RowIterator
#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     com_intel_oap_row_RowIterator
 * Method:    nativeHasNext
 * Signature: (J)Z
 */
JNIEXPORT jboolean JNICALL Java_com_intel_oap_row_RowIterator_nativeHasNext
  (JNIEnv *, jobject, jlong);

/*
 * Class:     com_intel_oap_row_RowIterator
 * Method:    nativeNext
 * Signature: (J)Lcom/intel/oap/row/SparkRowInfo;
 */
JNIEXPORT jobject JNICALL Java_com_intel_oap_row_RowIterator_nativeNext
  (JNIEnv *, jobject, jlong);

/*
 * Class:     com_intel_oap_row_RowIterator
 * Method:    nativeClose
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_com_intel_oap_row_RowIterator_nativeClose
  (JNIEnv *, jobject, jlong);

/*
 * Class:     com_intel_oap_row_RowIterator
 * Method:    nativeFetchMetrics
 * Signature: (J)Lcom/intel/oap/vectorized/MetricsObject;
 */
JNIEXPORT jobject JNICALL Java_com_intel_oap_row_RowIterator_nativeFetchMetrics
  (JNIEnv *, jobject, jlong);

#ifdef __cplusplus
}
#endif
#endif
