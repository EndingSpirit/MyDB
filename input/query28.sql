SELECT R.H, S.B, B.D FROM Reserves R, Sailors S, Boats B WHERE R.G >= 1 AND R.G <= 3 AND S.B = 200 AND B.D = R.H AND S.A = R.G;