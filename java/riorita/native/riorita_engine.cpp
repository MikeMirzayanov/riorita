#include "com_codeforces_riorita_engine_RioritaEngine.h"
#include "compact.h"

#include <exception>
#include <map>
#include <stdexcept>

using namespace std;
using namespace riorita;

std::string to_string(JNIEnv* env, jstring jstr) {
    if (NULL == jstr)
        throw runtime_error("Riorita: null string");

    const char* buffer = env->GetStringUTFChars(jstr, NULL);
    if (NULL == buffer)
        throw runtime_error("Riorita: unable to read Java string");

    string s(buffer);
    env->ReleaseStringUTFChars(jstr, buffer);
    return s;
}

map<long long, FileSystemCompactStorage*> storages;

inline long long get_storage_id(JNIEnv* env, jobject self) {
    jfieldID fid = env->GetFieldID(env->GetObjectClass(self), "id", "J");
    return env->GetLongField(self, fid);
}

inline FileSystemCompactStorage*& storageRef(JNIEnv* env, jobject self) {
    return storages[get_storage_id(env, self)];
}

inline FileSystemCompactStorage* getStorage(JNIEnv* env, jobject self) {
    FileSystemCompactStorage* result = storageRef(env, self);
    if (0 == result)
        throw runtime_error("Riorita: storage is not initialized");
    return result;
}

void throwNewRuntimeException(JNIEnv* env, const string& message) {
    env->ThrowNew(env->FindClass("java/lang/RuntimeException"), message.c_str());    
}

void throwCurrentException(JNIEnv* env) {
    try {
        throw;
    } catch (const std::exception& e) {
        throwNewRuntimeException(env, e.what());
    } catch (...) {
        throwNewRuntimeException(env, "Riorita: unknown native exception");
    }
}

JNIEXPORT void JNICALL Java_com_codeforces_riorita_engine_RioritaEngine_initialize
        (JNIEnv* env, jobject self, jstring directory, jint group_count) {
    try {
        storages[get_storage_id(env, self)] = new FileSystemCompactStorage(to_string(env, directory), int(group_count));
    } catch (...) {
        throwCurrentException(env);
    }
}

JNIEXPORT jboolean JNICALL Java_com_codeforces_riorita_engine_RioritaEngine_has
        (JNIEnv* env, jobject self, jstring section, jstring key, jlong current_timestamp) {
    try {
        return getStorage(env, self)->has(to_string(env, section), to_string(env, key), timestamp(current_timestamp));
    } catch (...) {
        throwCurrentException(env);
        return false;
    }
}

JNIEXPORT jbyteArray JNICALL Java_com_codeforces_riorita_engine_RioritaEngine_get
        (JNIEnv* env, jobject self, jstring section, jstring key, jlong current_timestamp) {
    try {
        string data;
        if (!getStorage(env, self)->get(to_string(env, section), to_string(env, key), timestamp(current_timestamp), data))
            return NULL;
        else {
            jbyteArray result = env->NewByteArray(jsize(data.length()));
            env->SetByteArrayRegion(result, 0, jsize(data.length()), (jbyte*)data.c_str());
            return result;
        }
    } catch (...) {
        throwCurrentException(env);
        return NULL;
    }
}

JNIEXPORT jboolean JNICALL Java_com_codeforces_riorita_engine_RioritaEngine_put
        (JNIEnv* env, jobject self, jstring section, jstring key, jbyteArray data, jlong current_timestamp, jlong lifetime, jboolean overwrite) {
    try {
        if (NULL == data) {
            getStorage(env, self)->erase(to_string(env, section), to_string(env, key), current_timestamp);
            return true;
        } else {
            char* b = (char*)env->GetByteArrayElements(data, NULL);
            if (NULL == b)
                return false;

            try {
                string _data(b, b + env->GetArrayLength(data));
                bool result = getStorage(env, self)->put(to_string(env, section), to_string(env, key), _data,
                    timestamp(current_timestamp), timestamp(lifetime), overwrite);
                env->ReleaseByteArrayElements(data, (jbyte*)b, JNI_ABORT);
                return result;
            } catch (...) {
                env->ReleaseByteArrayElements(data, (jbyte*)b, JNI_ABORT);
                throw;
            }
        }
    } catch (...) {
        throwCurrentException(env);
        return false;
    }
}

JNIEXPORT jboolean JNICALL Java_com_codeforces_riorita_engine_RioritaEngine_erase__Ljava_lang_String_2Ljava_lang_String_2J
        (JNIEnv* env, jobject self, jstring section, jstring key, jlong current_timestamp) {
    try {
        return getStorage(env, self)->erase(to_string(env, section), to_string(env, key), current_timestamp);
    } catch (...) {
        throwCurrentException(env);
        return false;
    }
}

JNIEXPORT void JNICALL Java_com_codeforces_riorita_engine_RioritaEngine_erase__Ljava_lang_String_2
        (JNIEnv* env, jobject self, jstring section) {
    try {
        getStorage(env, self)->erase(to_string(env, section));
    } catch (...) {
        throwCurrentException(env);
    }
}

JNIEXPORT void JNICALL Java_com_codeforces_riorita_engine_RioritaEngine_clear
        (JNIEnv* env, jobject self) {
    try {
        getStorage(env, self)->clear();
    } catch (...) {
        throwCurrentException(env);
    }
}
