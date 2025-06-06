- Add unique entity id to api keys so we can have multiple api keys resolve to the same entity in the entire system
   this way users can delete api keys and have multiple resolve to same pool. 

   Do this my forcing a "uniqueness" constraint on the existing entity name along with some string sanitization
   so we can ensure that its unique

   Once we add those rules, we need to modiy every model's key gen routine to strictly use the entity id when
   storing/ retrieving things rather than the uuid

