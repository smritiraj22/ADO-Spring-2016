all:	test_assign4_1.exe
test_assign4_1.exe: test_assign4_1.c
	gcc -g dberror.c storage_mgr.c int_buffer_manager.c test_assign4_1.c -o test_assign4_1 buffer_mgr.c dt.h buffer_mgr_stat.c tables.h test_helper.h rm_serializer.c btree_mgr.c btree_mgr.h -lm
clean:
	rm	storage_mgr.h.gch test_assign3_1.o dberror.h.gch test_helper.h.gch buffer_mgr.h.gch
