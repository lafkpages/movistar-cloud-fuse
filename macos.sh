# For building
export CPATH="/Library/Application Support/fuse-t/include/fuse"

# You may also need to:
# sudo ln -s fuse-t.pc /usr/local/lib/pkgconfig/fuse.pc

# Or maybe:
# install_name_tool -change @rpath/libfuse-t.dylib /usr/local/lib/libfuse-t.dylib ./node_modules/@cocalc/fuse-native/build/Release/fuse.node

# For running
export DYLD_LIBRARY_PATH="/Library/Application Support/fuse-t/lib:/usr/local/lib"
