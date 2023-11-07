#!/bin/bash

# 设置循环次数
max_runs=30
current_run=1
test_func=TestBackup2B

while [ "$current_run" -le "$max_runs" ]
do
    
    echo "=======Running test $test_func, attempt $current_run...======="
    
    # 运行测试命令，并将stdout和stderr保存到临时文件，同时将其还原到原始位置
    # output=$(go test -race -run $test_func 2>&1 | tee /dev/tty)
    output=$(go test -race -run $test_func 2>&1)

    # 检查stdout和stderr是否包含"fail"关键字
    if (echo "$output" | grep -q "FAIL"); then
        echo "Test failed! Exiting loop."
        echo "$output"
        break
    fi
    echo -e "=======Test passed in attempt $current_run..=======\n"
    
    # 增加运行计数
    ((current_run++))
done

if [ "$current_run" -gt "$max_runs" ]; then
    echo "Test $test_func passed in all $max_runs attempts."
fi
