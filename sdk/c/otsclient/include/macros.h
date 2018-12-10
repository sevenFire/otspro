#ifndef OTS_MACROS_H_
#define OTS_MACROS_H_

#ifdef __cplusplus
extern "C" {
#endif

/**
 * The following code block define OTS_API as the tag for exported
 * functions. The library should be compiled with symbols visibility
 * set to hidden by default and only the exported functions should be
 * tagged as OTS_API.
 *
 * When building the library on Windows, compile with compiler flag
 * "-D_OTS_IMPLEMENTATION_", whereas when linking application with
 * this library, this compiler flag should not be used.
 */
#if defined _WIN32 || defined __CYGWIN__
  #ifdef _OTS_IMPLEMENTATION_
      #define OTS_API __declspec(dllexport)
  #else
    #ifdef USE_STATIC_OTS
      #define OTS_API
    #else
      #define OTS_API __declspec(dllimport)
    #endif
  #endif
#else
  #if __GNUC__ >= 4
    #define OTS_API __attribute__ ((visibility ("default")))
  #else
    #define OTS_API
  #endif
#endif

#ifdef __cplusplus
}  /* extern "C" */
#endif

#endif /* OTS_MACROS_H_ */
