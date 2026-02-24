# For building
export CPATH="/Library/Application Support/fuse-t/include/fuse"

# You may also need to:
# sudo ln -s fuse-t.pc /usr/local/lib/pkgconfig/fuse.pc

# For running
export DYLD_LIBRARY_PATH="/Library/Application Support/fuse-t/lib:/usr/local/lib"
