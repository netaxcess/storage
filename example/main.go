package main

import (
    "github.com/netaxcess/storage/mem"
    "fmt"
)

func main() {
    s := mem.NewStorage()
    s.Set([]byte("L001"), []byte("数据1"))
    s.Set([]byte("L002"), []byte("数据2"))
    s.Set([]byte("L004"), []byte("数据3"))
    s.Set([]byte("L003"), []byte("数据4"))
    s.Set([]byte("L005"), []byte("数据5"))
    //批量获取指定的KEY
    v, _ := s.MGet([]byte("a"), []byte("b"), []byte("c"))
    for _ , vv := range v {
        //fmt.Println(string(vv))
    }
    
    //通过范围查找KEY的值
	s.Scan([]byte("L001"), []byte("L005"), func(key, value []byte) (bool, error) {
		fmt.Println(string(key),"==",string(value))
		
		return true, nil
	}, true)

}