{
  "targets": [
    {
      "target_name": "db",
      "type": "executable",
      "sources": [ "Database.cpp", "PacketBuffer.cpp", "Store.cpp", "taskQueue.cpp", "observation.cpp", "main.cpp" ],
      "cflags_cc": [
        "-std=c++17",
        "-Wall", "-Werror",
        "-Wno-unused-variable", "-Wno-unused-lambda-capture", "-Wno-sign-compare",
        "-fexceptions"
      ],
      "libraries": [
        "usockets"
      ]
    }
  ]
}
