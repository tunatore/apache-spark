%sql SELECT D.query, AVG(ROUND((D.count - D.LAST_QUERY_COUNT) / D.LAST_QUERY_COUNT, 2) * 100) percent
  FROM (SELECT DATE_FORMAT(query_time, 'yyyy-MM-dd') as query_time,LOWER(query) as query, COUNT(query) as count,
  LAG(COUNT(query), 1, COUNT(query)) OVER (PARTITION BY LOWER(query) ORDER BY DATE_FORMAT(query_time, 'yyyy-MM-dd')) as LAST_QUERY_COUNT
  FROM kaggle_geo
  WHERE DATE_FORMAT(query_time, 'yyyy-MM-dd') BETWEEN '${start='2011-09-09'}' AND '${end='2011-09-10'}'
  GROUP BY LOWER(query), DATE_FORMAT(query_time, 'yyyy-MM-dd')
  ORDER BY query, query_time) D GROUP BY query ORDER BY percent DESC LIMIT 10