SELECT * FROM Boats B1, Boats B2, Boats B3, Sailors S, Sailors S1 where B1.E = B2.F and S.A = B3.E and S1.A != B2.F and S1.A = B1.F ORDER BY S.A, B1.E, B2.F; 