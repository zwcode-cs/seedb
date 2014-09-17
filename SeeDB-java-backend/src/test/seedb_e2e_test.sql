CREATE TABLE seedb_e2e_test (
	dim1 VARCHAR(40),
	dim2 VARCHAR(40),
	dim3 VARCHAR(40),
	dim4 VARCHAR(40),
	measure1 INTEGER,
	measure2 INTEGER,
	measure3 INTEGER,
	measure4 INTEGER
);

INSERT INTO seedb_e2e_test VALUES
('abc', 'abc', 'abc', 'pqr', 100, 5000, 300, 2500),
('abc', 'def', 'abc', 'stu', 700, 2000, 1000, 5500),
('def', 'def', 'abc', 'stu', 900, 1000, 5000, 6500),
('def', 'def', 'abc', 'vwx', 300, 4000, 900, 3500),
('def', 'def', 'ghi', 'vwx', 500, 7000, 800, 4000),
('ghi', 'def', 'jkl', 'vwx', 1000, 1500, 700, 750),
('jkl', 'def', 'mno', 'yza', 200, 3000, 400, 7500),
('jkl', 'ghi', 'mno', 'yza', 800, 9000, 500, 8500),
('jkl', 'ghi', 'mno', 'yza', 1300, 8000, 700, 9500),
('jkl', 'ghi', 'mno', 'yza', 300, 5000, 5000, 2500);