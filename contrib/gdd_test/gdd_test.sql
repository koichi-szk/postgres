CREATE OR REPLACE FUNCTION gdd_test_init_test()
	RETURNS INT
	LANGUAGE c VOLATILE
	AS 'gdd_test.so', 'gdd_test_init_test';

CREATE OR REPLACE FUNCTION gdd_test_finish_test()
	RETURNS INT
	LANGUAGE c VOLATILE
	AS 'gdd_test.so', 'gdd_test_finish_test';

CREATE OR REPLACE FUNCTION gdd_test_show_myself()
	RETURNS TABLE(system_id BIGINT, pid INT, pgprocno INT, lxn INT)
	LANGLAGE c VOLATILE
	AS 'gdd_test.so', 'gdd_test_show_myself';

CREATE OR REPLACE FUNCTION gdd_test_show_proc(pid int)
	RETURNS TABLE(system_id BIGINT, pid INT, pgprocno INT, lxn INT)
	LANGLAGE c VOLATILE
	AS 'gdd_test.so', 'gdd_test_show_proc';

CREATE OR REPLACE FUNCTION
	gdd_test_set_locktag_external_pgprocno
		(label text, pgorocno int, incr bool)
	RETURNS TABLE  (label text, field1 int, field2 int, field3 int, field4 int, type text, lockmethod int)
	LANGAGE c VOLATILE
	AS 'gdd_test.so', 'gdd_test_set_locktag_external_pgprocno';

CREATE OR REPLACE FUNCTION
	gdd_test_set_locktag_external_pid
		(label text, pid int, incr bool)
	RETURNS TABLE  (label text, field1 int, field2 int, field3 int, field4 int, type text, lockmethod int)
	LANGAGE c VOLATILE
	AS 'gdd_test.so', 'gdd_test_set_locktag_external_pid';

