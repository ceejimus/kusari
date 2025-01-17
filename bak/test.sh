echo > ./.data/time.log
# for buf_size in 64 128 254 512 1024 2048 4196 8192 16384; do
for buf_size in 16384; do
  # for chan_cap in 1 4 16 64; do
  for chan_cap in 16; do
    # for i in 0 1 2; do
    for i in 0; do
      # echo "null" $buf_size $chan_cap | tee -a ./.data/time.log
      # { echo | (printf "%08x%08x" $buf_size $chan_cap | xxd -r -p; cat) | time nc 0.0.0.0 7337 > /dev/null; } 2>> ./.data/time.log
      echo "to_file" $buf_size $chan_cap | tee -a ./.data/time.log
      { echo | (printf "%08x%08x" $buf_size $chan_cap | xxd -r -p; cat) | time nc 0.0.0.0 7337 > ./.data/test.out; } 2>> ./.data/time.log
      diff ./.data/bigfile.dat ./.data/test.out
      if [ $? -ne 0 ]; then
        echo "FILE DOWNLOAD ERROR"
      fi
    done
  done
done
