SELECT Sailors.B, Sailors.C FROM Sailors, Reserves WHERE Sailors.A = Reserves.G GROUP BY Sailors.B, Sailors.C ORDER BY Sailors.C, Sailors.B;
