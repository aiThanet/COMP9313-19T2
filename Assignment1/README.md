# COMP9313-Assignment1

## install

For macOS : https://isaacchanghau.github.io/post/install_hadoop_mac/

## run

hadoop jar assignment1.jar <ngram> <minimum count> <input_path_in_hdfs> <output_path_in_hdfs>
  
```
hadoop jar assignment1.jar 1 4 /user/thanetsirichanyaphong/input /user/thanetsirichanyaphong/output
```

## helpful
example input and output
input
example_output file




## install issues

1. macOS ssh local always ask for password
- fixed : https://superuser.com/questions/1424650/cannot-do-passwordless-ssh-login-to-macos

2. resource manager node / data node are not start (checked by ```$ jps```)
- fixed : remove tmp folder and start again

3. hadoop 3.x.x only support java8
