Project Data Pipeline for Recruitment Start Up 

Objectives : Tính toán tổng hợp số lượt click , số ứng viên ứng tuyển , số người đạt chuẩn , không đạt chuẩn của các job được chạy trên website.
Data Flow : 

![Picture1](https://github.com/dinhtatthanh212/Build-a-Data-Pipeline-and-near-real-time-dashboard-for-Recruitment-Start-Up-/assets/138422627/aeaa8ced-b710-4c9e-9def-f2769ea3288d)

Data Structure 
In datalake : 
Chứa dữ liệu thô ( logs data ) về 1 cái event vừa xảy ra 




![Picture2](https://github.com/dinhtatthanh212/Build-a-Data-Pipeline-and-near-real-time-dashboard-for-Recruitment-Start-Up-/assets/138422627/44a26dfd-96da-44a5-ab6d-ad4b9613bffc)









In data warehouse ( Event đã được tổng hợp ) 
( kiểu vào giờ đó , ngày đó , cái job này , có bao nhiêu người click vào , của campaign nào , nhà tuyển dụng nào , nhóm nào ,... ) 

 

Input : Logs từ datalake , các bảng dimension trong Datawarehouse 
Output : Events table trong Datawarehouse 

Requirements : Chạy realtime hoặc near real time , và job được viết bằng Spark ( Spark Job ) 
