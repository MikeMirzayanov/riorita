#include "com_codeforces_riorita_engine_RioritaEngine.h"
#include "compact.h"

#include <map>

using namespace std;
using namespace riorita;

std::string to_string(JNIEnv* env, jstring jstr) {
    const char* buffer = env->GetStringUTFChars(jstr, NULL);
    string s(buffer);
    env->ReleaseStringUTFChars(jstr, buffer);
    return s;
}

map<long long, FileSystemCompactStorage*> storages;

inline long long get_storage_id(JNIEnv* env, jobject self) {
    jfieldID fid = env->GetFieldID(env->GetObjectClass(self), "id", "J");
    return env->GetIntField(self, fid);
}

inline FileSystemCompactStorage*& storage(JNIEnv* env, jobject self) {
    return storages[get_storage_id(env, self)];
}

void throwNewRuntimeException(JNIEnv* env, const string& message) {
    env->ThrowNew(env->FindClass("java/lang/RuntimeException"), message.c_str());    
}

JNIEXPORT void JNICALL Java_com_codeforces_riorita_engine_RioritaEngine_initialize
        (JNIEnv* env, jobject self, jstring directory, jint group_count) {
    storages[get_storage_id(env, self)] = new FileSystemCompactStorage(to_string(env, directory), int(group_count));
}

JNIEXPORT jboolean JNICALL Java_com_codeforces_riorita_engine_RioritaEngine_has
        (JNIEnv* env, jobject self, jstring section, jstring key, jlong current_timestamp) {
    return storage(env, self)->has(to_string(env, section), to_string(env, key), timestamp(current_timestamp));
}

JNIEXPORT jbyteArray JNICALL Java_com_codeforces_riorita_engine_RioritaEngine_get
        (JNIEnv* env, jobject self, jstring section, jstring key, jlong current_timestamp) {
    string data;
    if (NULL == storage(env, self)->get(to_string(env, section), to_string(env, key), timestamp(current_timestamp), data))
        return NULL;
    else {
        jbyteArray result = env->NewByteArray(jsize(data.length()));
        env->SetByteArrayRegion(result, 0, jsize(data.length()), (jbyte*)data.c_str());
        return result;
    }
}

JNIEXPORT jboolean JNICALL Java_com_codeforces_riorita_engine_RioritaEngine_put
        (JNIEnv* env, jobject self, jstring section, jstring key, jbyteArray data, jlong current_timestamp, jlong lifetime, jboolean overwrite) {
    if (NULL == data) {
        storage(env, self)->erase(to_string(env, section), to_string(env, key), current_timestamp);
        return true;
    } else {
        char* b = (char*)env->GetByteArrayElements(data, NULL);
        string _data(b, b + env->GetArrayLength(data));
        bool result = storage(env, self)->put(to_string(env, section), to_string(env, key), _data,
            timestamp(current_timestamp), timestamp(lifetime), overwrite);
        env->ReleaseByteArrayElements(data, (jbyte*)b, 0);
        return result;
    }
}

JNIEXPORT jboolean JNICALL Java_com_codeforces_riorita_engine_RioritaEngine_erase__Ljava_lang_String_2Ljava_lang_String_2J
        (JNIEnv* env, jobject self, jstring section, jstring key, jlong current_timestamp) {
    return storage(env, self)->erase(to_string(env, section), to_string(env, key), current_timestamp);
}

JNIEXPORT void JNICALL Java_com_codeforces_riorita_engine_RioritaEngine_erase__Ljava_lang_String_2
        (JNIEnv* env, jobject self, jstring section) {
    try {
        storage(env, self)->erase(to_string(env, section));
    } catch (const std::exception& e) {
        throwNewRuntimeException(env, e.what());
    }
}

JNIEXPORT void JNICALL Java_com_codeforces_riorita_engine_RioritaEngine_clear
        (JNIEnv* env, jobject self) {
    long long id = get_storage_id(env, self);
    FileSystemCompactStorage* storage = storages[id];
    if (0 != storage) {
        string dir(storage->get_dir());
        int groups(storage->get_groups());
        storage->close();
        delete storage;
        storages[id] = new FileSystemCompactStorage(dir, groups);
    }
}
