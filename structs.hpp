struct slice {
  int slice_index;
  int data_len;
  int data_start;
  int pos_final;
  slice(int index, int len, int start, int pos)
      : slice_index(index), data_len(len), data_start(start), pos_final(pos){};
};

