DROP TABLE IF EXISTS seedb_e2e_test;
CREATE TABLE seedb_e2e_test (
	id INTEGER,
	dim1_3 VARCHAR(40),
	dim2_3 VARCHAR(40),
	dim3_4 VARCHAR(40),
	dim4_4 VARCHAR(40),
	measure1 INTEGER,
	measure2 INTEGER,
	measure3 INTEGER,
	measure4 INTEGER
);

INSERT INTO seedb_e2e_test VALUES
(1, 'abc', 'abc', 'abc', 'pqr', 100, 5000, 300, 2500),
(2, 'abc', 'def', 'abc', 'stu', 700, 2000, 1000, 5500),
(3, 'def', 'def', 'abc', 'stu', 900, 1000, 5000, 6500),
(4, 'def', 'def', 'abc', 'vwx', 300, 4000, 900, 3500),
(5, 'def', 'def', 'ghi', 'vwx', 500, 7000, 800, 4000),
(6, 'ghi', 'def', 'jkl', 'vwx', 1000, 1500, 700, 750),
(7, 'jkl', 'def', 'mno', 'yza', 200, 3000, 400, 7500),
(8, 'jkl', 'ghi', 'mno', 'yza', 800, 9000, 500, 8500),
(9, 'jkl', 'ghi', 'mno', 'yza', 1300, 8000, 700, 9500),
(10, 'jkl', 'ghi', 'mno', 'yza', 300, 5000, 5000, 2500);