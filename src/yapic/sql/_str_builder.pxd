

cdef extern from "_str_builder.hpp":
    cdef cppclass UnicodeBuilder:
        UnicodeBuilder()
        void AppendAscii(char ch)
        void AppendChar(char ch)
        bint AppendCharSafe(char ch) except 0
        bint AppendString(const char* str) except 0
        bint AppendString(const char* str, size_t size) except 0
        bint AppendStringSafe(const char* str) except 0
        bint AppendStringSafe(const char* str, size_t size) except 0
        bint AppendString(object str) except 0
        bint AppendStringSafe(object str) except 0
        bint AppendBytes(object bytes) except 0
        bint AppendBytesSafe(object bytes) except 0
        bint EnsureSize(size_t size) except 0
        object ToPython()
