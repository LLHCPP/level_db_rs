use std::fmt;

const K_NUM_BUCKETS: usize = 154;
static K_BUCKET_LIMIT: [f64; 154] = [
    1f64,
    2f64,
    3f64,
    4f64,
    5f64,
    6f64,
    7f64,
    8f64,
    9f64,
    10f64,
    12f64,
    14f64,
    16f64,
    18f64,
    20f64,
    25f64,
    30f64,
    35f64,
    40f64,
    45f64,
    50f64,
    60f64,
    70f64,
    80f64,
    90f64,
    100f64,
    120f64,
    140f64,
    160f64,
    180f64,
    200f64,
    250f64,
    300f64,
    350f64,
    400f64,
    450f64,
    500f64,
    600f64,
    700f64,
    800f64,
    900f64,
    1000f64,
    1200f64,
    1400f64,
    1600f64,
    1800f64,
    2000f64,
    2500f64,
    3000f64,
    3500f64,
    4000f64,
    4500f64,
    5000f64,
    6000f64,
    7000f64,
    8000f64,
    9000f64,
    10000f64,
    12000f64,
    14000f64,
    16000f64,
    18000f64,
    20000f64,
    25000f64,
    30000f64,
    35000f64,
    40000f64,
    45000f64,
    50000f64,
    60000f64,
    70000f64,
    80000f64,
    90000f64,
    100000f64,
    120000f64,
    140000f64,
    160000f64,
    180000f64,
    200000f64,
    250000f64,
    300000f64,
    350000f64,
    400000f64,
    450000f64,
    500000f64,
    600000f64,
    700000f64,
    800000f64,
    900000f64,
    1000000f64,
    1200000f64,
    1400000f64,
    1600000f64,
    1800000f64,
    2000000f64,
    2500000f64,
    3000000f64,
    3500000f64,
    4000000f64,
    4500000f64,
    5000000f64,
    6000000f64,
    7000000f64,
    8000000f64,
    9000000f64,
    10000000f64,
    12000000f64,
    14000000f64,
    16000000f64,
    18000000f64,
    20000000f64,
    25000000f64,
    30000000f64,
    35000000f64,
    40000000f64,
    45000000f64,
    50000000f64,
    60000000f64,
    70000000f64,
    80000000f64,
    90000000f64,
    100000000f64,
    120000000f64,
    140000000f64,
    160000000f64,
    180000000f64,
    200000000f64,
    250000000f64,
    300000000f64,
    350000000f64,
    400000000f64,
    450000000f64,
    500000000f64,
    600000000f64,
    700000000f64,
    800000000f64,
    900000000f64,
    1000000000f64,
    1200000000f64,
    1400000000f64,
    1600000000f64,
    1800000000f64,
    2000000000f64,
    2500000000.0f64,
    3000000000.0f64,
    3500000000.0f64,
    4000000000.0f64,
    4500000000.0f64,
    5000000000.0f64,
    6000000000.0f64,
    7000000000.0f64,
    8000000000.0f64,
    9000000000.0f64,
    1e200f64,
];
struct Histogram {
    data: Vec<f64>,
    min_: f64,
    max_: f64,
    num_: f64,
    sum_: f64,
    sum_squares_: f64,
    buckets_: [f64; K_NUM_BUCKETS],
}
impl Histogram {
    fn clear(&mut self) {
        self.min_ = K_BUCKET_LIMIT[K_NUM_BUCKETS - 1];
        self.max_ = 0f64;
        self.num_ = 0f64;
        self.sum_ = 0f64;
        self.sum_squares_ = 0f64;
        for i in 0..K_NUM_BUCKETS {
            self.buckets_[i] = 0f64;
        }
    }

    fn add(&mut self, value: f64) {
        let mut b = 0;
        while b < K_NUM_BUCKETS - 1 && K_BUCKET_LIMIT[b] <= value {
            b += 1;
        }
        self.buckets_[b] += 1.0f64;
        if self.min_ > value {
            self.min_ = value;
        }
        if self.max_ < value {
            self.max_ = value;
        }
        self.num_ += 1.0f64;
        self.sum_ += value;
        self.sum_squares_ += value * value;
    }

    fn merge(&mut self, other: &Histogram) {
        if other.min_ < self.min_ {
            self.min_ = other.min_;
        }
        if other.max_ > self.max_ {
            self.max_ = other.max_;
        }
        self.num_ += other.num_;
        self.sum_ += other.sum_;
        self.sum_squares_ += other.sum_squares_;
        for b in 0..K_NUM_BUCKETS {
            self.buckets_[b] += other.buckets_[b];
        }
    }

    fn percentile(&self, p: f64) -> f64 {
        let threshold = self.num_ * (p / 100.0f64);
        let mut sum = 0f64;
        for b in 0..K_NUM_BUCKETS {
            sum += self.buckets_[b];
            if sum >= threshold {
                let left_point = if b == 0 { 0f64 } else { K_BUCKET_LIMIT[b - 1] };
                let right_point = K_BUCKET_LIMIT[b];
                let left_sum = sum - self.buckets_[b];
                let right_sum = sum;
                let pos = (threshold - left_sum) / (right_sum - left_sum);
                let mut r = left_point + pos * (right_point - left_point);
                if r < self.min_ {
                    r = self.min_;
                }
                if r > self.max_ {
                    r = self.max_;
                }
                return r;
            }
        }
        self.max_
    }

    fn median(&self) -> f64 {
        self.percentile(50.0f64)
    }

    fn average(&self) -> f64 {
        if self.num_ == 0.0f64 {
            0.0f64
        } else {
            self.sum_ / self.num_
        }
    }

    fn standard_deviation(&self) -> f64 {
        if self.num_ == 0.0f64 {
            0.0f64
        } else {
            let variance =
                (self.sum_squares_ * self.num_ - self.sum_ * self.sum_) / (self.num_ * self.num_);
            variance.sqrt()
        }
    }
}

impl fmt::Display for Histogram {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // 第一行：Count, Average, StdDev
        let mut output = String::new();
        output.push_str(&format!(
            "Count: {:.0}  Average: {:.4}  StdDev: {:.2}\n",
            self.num_,
            self.average(),
            self.standard_deviation()
        ));

        // 第二行：Min, Median, Max
        let min_value = if self.num_ == 0.0 { 0.0 } else { self.min_ };
        output.push_str(&format!(
            "Min: {:.4}  Median: {:.4}  Max: {:.4}\n",
            min_value,
            self.median(),
            self.max_
        ));

        // 分隔线
        output.push_str("------------------------------------------------------\n");

        // 桶的统计
        let mult = 100.0 / self.num_;
        let mut sum = 0.0;
        for b in 0..K_NUM_BUCKETS {
            if self.buckets_[b] <= 0.0 {
                continue;
            }
            sum += self.buckets_[b];

            // 桶的左右边界
            let left = if b == 0 { 0.0 } else { K_BUCKET_LIMIT[b - 1] };
            let right = K_BUCKET_LIMIT[b];

            // 格式化桶信息
            output.push_str(&format!(
                "[ {:7.0}, {:7.0} ) {:7.0} {:7.3}% {:7.3}% ",
                left,
                right,
                self.buckets_[b],
                mult * self.buckets_[b],
                mult * sum
            ));

            // 添加 # 标记，20 个 # 表示 100%
            let marks = (20.0 * (self.buckets_[b] / self.num_) + 0.5) as usize;
            output.push_str(&"#".repeat(marks));
            output.push('\n');
        }

        // 将结果写入格式化器
        write!(f, "{}", output)
    }
}
