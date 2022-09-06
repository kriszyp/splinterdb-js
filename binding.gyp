{
  "variables": {
      "os_linux_compiler%": "gcc",
      "use_robust%": "true",
      "use_data_v1%": "false",
      "enable_v8%": "true",
      "enable_fast_api_calls%": "true",
      "enable_pointer_compression%": "false",
      "openssl_fips": "X",
      "target%": "",
      "build_v8_with_gn": "false",
      "runtime%": "node"
  },
  "conditions": [
    ['OS=="win"', {
      "variables": {
        "enable_fast_api_calls%": "<!(echo %ENABLE_FAST_API_CALLS%)",
        "enable_v8%": "<!(echo %ENABLE_V8_FUNCTIONS%)",
      }
    }],
    ['OS!="win"', {
      "variables": {
        "enable_fast_api_calls%": "<!(echo $ENABLE_FAST_API_CALLS)",
        "enable_v8%": "<!(echo $ENABLE_V8_FUNCTIONS)",
      }
    }]
  ],
  "targets": [
    {
      "target_name": "splinterdb",
      "sources": [
        "dependencies/splinterdb/src/*.c",
        "dependencies/lz4/lib/lz4.h",
        "dependencies/lz4/lib/lz4.c",
        "src/env.cpp",
      ],
      "include_dirs": [
        "<!(node -p \"require('node-addon-api').include_dir\")",
        "dependencies/splinterdb/include",
        "dependencies/splinterdb/src",
        "dependencies/splinterdb/src/platform_linux",
        "dependencies/xxHash",
        "dependencies/lz4/lib"
      ],
      "defines": ["SPLINTERDB_PLATFORM_DIR=platform_linux", "NAPI_DISABLE_CPP_EXCEPTIONS" ],
      "conditions": [
        ["OS=='linux'", {
          "variables": {
            "gcc_version" : "<!(<(os_linux_compiler) -dumpversion | cut -d '.' -f 1)",
          },
          "cflags_cc": [
            "-fPIC",
            "-Wno-strict-aliasing",
            "-Wno-unused-result",
            "-Wno-cast-function-type",
            "-fvisibility=hidden",
            "-fvisibility-inlines-hidden",
          ],
          "ldflags": [
            "-fPIC",
            "-fvisibility=hidden"
          ],
          "cflags": [
            "-fPIC",
            "-fvisibility=hidden",
            "-O3"
          ],
        }],
        ["enable_pointer_compression=='true'", {
          "defines": ["V8_COMPRESS_POINTERS", "V8_COMPRESS_POINTERS_IN_ISOLATE_CAGE"],
        }],
        ['runtime=="electron"', {
          "defines": ["NODE_RUNTIME_ELECTRON=1"]
        }],
        ["enable_v8!='false'", {
          "defines": ["ENABLE_V8_API=1"],
        }],
        ["enable_fast_api_calls=='true'", {
          "defines": ["ENABLE_FAST_API_CALLS=1"],
        }],
        ["use_robust=='true'", {
          "defines": ["MDB_USE_ROBUST"],
        }],
      ],
    }
  ]
}
