pub struct Random {
    seed: u32,
}

impl Random {
    // 构造函数，接受一个初始种子
    pub fn new(s: u32) -> Self {
        let mut seed = s & 0x7fffffff; // 限制为 31 位
                                       // 避免坏种子
        if seed == 0 || seed == 2147483647 {
            seed = 1;
        }
        Random { seed }
    }

    // 生成下一个伪随机数
    pub fn next(&mut self) -> u32 {
        const M: u32 = 2147483647; // 2^31 - 1
        const A: u64 = 16807; // 乘数

        // 计算 seed_ * A
        let product = (self.seed as u64) * A;

        // 计算 (product % M) 使用位运算优化
        self.seed = ((product >> 31) + (product & M as u64)) as u32;
        // 如果溢出，则减去 M
        if self.seed > M {
            self.seed -= M;
        }
        self.seed
    }

    // 返回 [0..n-1] 范围内均匀分布的随机数
    pub fn uniform(&mut self, n: u32) -> u32 {
        debug_assert!(n > 0, "n must be greater than 0");
        self.next() % n
    }

    // 以 1/n 的概率返回 true
    pub fn one_in(&mut self, n: u32) -> bool {
        debug_assert!(n > 0, "n must be greater than 0");
        self.next() % n == 0
    }

    // 生成偏向小值的随机数
    pub fn skewed(&mut self, max_log: i32) -> u32 {
        let base = self.uniform((max_log + 1) as u32);
        self.uniform(1 << base)
    }
}
