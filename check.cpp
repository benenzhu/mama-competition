#include "ModelSliceReader.h"

#include <cstdio>
#include <string> 
using namespace std;
int main(int , char**v){
  string s = v[1];
  
  ModelSliceReader A;
  A.Load(s);

  char buf[4];
  for(int i = 0; i <= 64; i+= 4) {
    A.Read(i, 4, buf);
    printf("%d %d\n", i, *((int *)buf));
  }
  return 0;

}
