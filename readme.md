# ETL Banks Information Project

## Introduction
Repo này lưu trữ toàn bộ source code cho project tìm hiểu và thực hành sử dụng Python để ETL dữ liệu về ngân hàng. 

## Scenario
Tổng hợp danh sách 10 ngân hàng lớn nhất thế giới xếp hạng theo vốn hóa thị trường tính bằng tỷ USD. Dữ liệu cần phải được chuyển đổi và lưu trữ bằng GBP, EUR và INR, theo thông tin tỷ giá hối đoái đã được cung cấp dưới dạng tệp CSV. Bảng thông tin đã xử lý sẽ được lưu ở định dạng CSV và dưới dạng bảng trong cơ sở dữ liệu. Hệ thống tự động tạo ra thông tin này để có thể thực hiện thông tin tương tự trong mỗi quý tài chính nhằm chuẩn bị báo cáo.

## Process Overview
| STT | Parameter | Value |
| --- | ----- | ----- |
| 1 | Data URL | https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks |
| 2 | Exchange rate CSV path | https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv |
| 3 | Tables Attributes (Extract only) | Name, MC_USD_Billion |
| 4 | Table Attributes (Final) | Name, MC_USD_Billion, MC_GBP_Billion, MC_EUR_Billion, MC_INR_Billion |
| 5 | Output CSV Path | ./Largest_banks_data.csv |
| 6 | Database name | Banks.db |
| 7 | Table name | Largest_banks |
| 8 | Log file | code_log.txt |

Thực hiện ETL dữ liệu và cung cấp các thông tin đã xử lý để sử dụng tiếp ở các định dạng khác nhau:
- Sử dụng WebScraping để trích xuất thông tin từ trang web
- Sử dụng Pandas data frames và dictionaries transform dữ liệu theo yêu cầu
- Load thông tin đã xử lý vào file CSV và bảng trong Database
- Query các bảng bằng SQLite3 và thư viện Pandas
- Ghi Log quy trình thực hiện, chạy code