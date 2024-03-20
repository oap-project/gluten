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

#include <stdexcept>
#include <string>
#include <jni/jni_common.h>
#include <jni/jni_error.h>

namespace local_engine
{
jclass CreateGlobalExceptionClassReference(JNIEnv * env, const char * class_name)
{
    jclass local_class = env->FindClass(class_name);
    jclass global_class = static_cast<jclass>(env->NewGlobalRef(local_class));
    env->DeleteLocalRef(local_class);
    if (global_class == nullptr)
    {
        std::string error_msg = "Unable to createGlobalClassReference for" + std::string(class_name);
        throw std::runtime_error(error_msg);
    }
    return global_class;
}

jclass CreateGlobalClassReference(JNIEnv * env, const char * class_name)
{
    jclass local_class = env->FindClass(class_name);
    jclass global_class = static_cast<jclass>(env->NewGlobalRef(local_class));
    env->DeleteLocalRef(local_class);
    if (global_class == nullptr)
    {
        std::string error_message = "Unable to createGlobalClassReference for" + std::string(class_name);
        env->ThrowNew(JniErrorsGlobalState::instance().getIllegalAccessExceptionClass(), error_message.c_str());
    }
    return global_class;
}

jmethodID GetMethodID(JNIEnv * env, jclass this_class, const char * name, const char * sig)
{
    jmethodID ret = env->GetMethodID(this_class, name, sig);
    if (ret == nullptr)
    {
        std::string error_message = "Unable to find method " + std::string(name) + " within signature" + std::string(sig);
        env->ThrowNew(JniErrorsGlobalState::instance().getIllegalAccessExceptionClass(), error_message.c_str());
    }

    return ret;
}

jmethodID GetStaticMethodID(JNIEnv * env, jclass this_class, const char * name, const char * sig)
{
    jmethodID ret = env->GetStaticMethodID(this_class, name, sig);
    if (ret == nullptr)
    {
        std::string error_message = "Unable to find static method " + std::string(name) + " within signature" + std::string(sig);
        env->ThrowNew(JniErrorsGlobalState::instance().getIllegalAccessExceptionClass(), error_message.c_str());
    }
    return ret;
}

jstring charTojstring(JNIEnv * env, const char * pat)
{
    jclass str_class = (env)->FindClass("Ljava/lang/String;");
    jmethodID ctor_id = (env)->GetMethodID(str_class, "<init>", "([BLjava/lang/String;)V");
    jsize strSize = static_cast<jsize>(strlen(pat));
    jbyteArray bytes = (env)->NewByteArray(strSize);
    (env)->SetByteArrayRegion(bytes, 0, strSize, reinterpret_cast<jbyte *>(const_cast<char *>(pat)));
    jstring encoding = (env)->NewStringUTF("UTF-8");
    jstring result = static_cast<jstring>((env)->NewObject(str_class, ctor_id, bytes, encoding));
    env->DeleteLocalRef(bytes);
    env->DeleteLocalRef(encoding);
    return result;
}

jbyteArray stringTojbyteArray(JNIEnv * env, const std::string & str)
{
    const auto * ptr = reinterpret_cast<const jbyte *>(str.c_str());
    jsize strSize = static_cast<jsize>(str.size());
    jbyteArray jarray = env->NewByteArray(strSize);
    env->SetByteArrayRegion(jarray, 0, strSize, ptr);
    return jarray;
}

}
