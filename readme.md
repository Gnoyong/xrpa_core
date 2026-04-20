## BI数据过程命令

### 当月

order:

```powershell
# 无需指定日期范围，默认获取当月第一天至今（洛杉矶时间）的数据
tkauto order fetch-affiliate 

tkauto order fetch-seller --start-date 2026-03-01 --end-date 2026-03-31
```

video:

```powershell
tkauto video daily

tkauto video fill-from-affiliate-order

tkauto video fill-bd
```

ads:

```powershell
tkauto ads fetch-dday
```

### 区间

order:

```powershell
tkauto order fetch-month

tkauto order seller-order
```

video:

```powershell
tkauto video daily

tkauto video fill-from-affiliate-order

tkauto video fill-bd
```

ads:

```powershell
tkauto ads fetch-dday
```

## Todo

 - [ ] 发样表单独维护一个bd字段，默认从对应的通过的申样获取bd，否则从归属表获取
