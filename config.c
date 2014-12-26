#include <libconfig.h>
#include <stdio.h>
#include <string.h>

void getConfString (char *key, char *value)
{
	config_t *conf = &(config_t) {};
	config_init (conf);
	config_read_file (conf, "apexRTS.conf");
	char *tmp;
	config_lookup_string (conf, key, (const char **)&tmp);
	memcpy (value, tmp, strlen (tmp));
	config_destroy (conf);
}
