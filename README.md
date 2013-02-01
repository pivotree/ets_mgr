ets_mgr
=======

Keep you ETS tables safe!

ets\_mgr is simple gen\_server that wraps some of the ETS interface.
When tables are constructed through the manager API it will
automatically declare itself as the heir to that table. If your calling
process ever dies your data is safe and can be retrieved when your
process is restarted.

Credits
-------
Inspired by
http://steve.vinoski.net/blog/2011/03/23/dont-lose-your-ets-tables/
