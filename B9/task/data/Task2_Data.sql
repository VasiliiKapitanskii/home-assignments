--Task 4
--You have data about check-in and check-out dates, you have to answer the question, how many rooms should be prepared and opened for each given week?
--The number of guests is always 2 per room and that a room can always be made available the same day as the check-out date.

CREATE TABLE reservations (
    res_id INT PRIMARY KEY,
    check_in_date DATE NOT NULL,
    check_out_date DATE NOT NULL
);

INSERT INTO reservations VALUES
    (1,  '2021-01-04', '2021-01-05'),
    (2,  '2021-01-05', '2021-01-07'),
    (3,  '2021-01-06', '2021-01-07'),
    (4,  '2021-01-06', '2021-01-08'),
    (5,  '2021-01-08', '2021-01-09'),
    (6,  '2021-01-08', '2021-01-09'),
    (7,  '2021-01-08', '2021-01-10'),
    (8,  '2021-01-13', '2021-01-17'),
    (9,  '2021-01-13', '2021-01-14'),
    (10, '2021-01-14', '2021-01-15'),
    (11, '2021-01-15', '2021-01-16'),
    (12, '2021-01-16', '2021-01-17');

--You result must be like below 
--week,		number_of_rooms
--2021-01,	3
--2021-02,	2