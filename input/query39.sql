SELECT DISTINCT * FROM Boats B, Reserves R, Sailors S WHERE R.G = S.A AND R.H <= 102 ORDER BY B.E,S.C;