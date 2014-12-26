#include <sqlite3.h>
#include <stdio.h>

void main()
{
	sqlite3 *db;
	char **result;
	int rows, cols;
	char *errmsg = NULL;

	if (sqlite3_open_v2 ("test.db", &db, SQLITE_OPEN_READWRITE, NULL))
		perror("fail");

	sqlite3_get_table(db, "select * from t2;", &result, &rows, &cols, &errmsg);

	int i, j;
	for (i = 0; i < rows + 1; i++)
	{
		for (j = 0; j < cols; j++)
		{
			printf ("%s \t", result[i * cols + j]);
		}
		printf ("\n");
	}
	return;
}
