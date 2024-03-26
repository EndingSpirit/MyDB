SELECT Reserves.H, SUM(Reserves.G * Reserves.G) FROM Reserves GROUP BY Reserves.H;
